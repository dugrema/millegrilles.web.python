import aiohttp
import asyncio
import datetime
import errno
import json
import logging
import pathlib
import tempfile
import shutil

from aiohttp import web
from aiohttp.web_request import Request, StreamResponse
from ssl import SSLContext, VerifyMode
from typing import Optional

from millegrilles_messages.messages import Constantes
from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_web import Constantes as ConstantesWeb
from millegrilles_web.EtatWeb import EtatWeb

BATCH_INTAKE_UPLOAD_DEFAULT = 100_000_000
INTAKE_CHUNK_SIZE = 64 * 1024


class EtatUploadParts:

    def __init__(self, fuuid: str, file_parts: list[pathlib.Path], stop_event: asyncio.Event, taille):
        self.fuuid = fuuid
        self.file_parts = file_parts
        self.fp_file = None  # fp du fichier courant
        self.stop_event = stop_event
        self.taille = taille
        self.position = 0
        self.samples = list()
        self.cb_activite = None
        self.done = False


class JobVerifierParts:

    def __init__(self, path_upload: pathlib.Path, hachage: str):
        self.path_upload = path_upload
        self.hachage = hachage
        self.done = asyncio.Event()
        self.valide: Optional[bool] = None
        self.exception: Optional[Exception] = None


async def feed_filepart2(etat_upload: EtatUploadParts, limit=BATCH_INTAKE_UPLOAD_DEFAULT):
    taille_uploade = 0

    if not etat_upload.fp_file:
        try:
            prochain_fichier = etat_upload.file_parts.pop(0)
            etat_upload.fp_file = prochain_fichier.open(mode='rb')
        except IndexError:
            etat_upload.done = True
            etat_upload.fp_file = None

    input_stream = etat_upload.fp_file

    while input_stream and taille_uploade < limit:
        if etat_upload.stop_event.is_set():
            break  # Stopped

        chunk = input_stream.read(INTAKE_CHUNK_SIZE)
        if not chunk:
            # Charger prochain fichier
            input_stream.close()
            etat_upload.fp_file = None
            try:
                prochain_fichier = etat_upload.file_parts.pop(0)
            except IndexError:
                etat_upload.done = True
                break
            etat_upload.fp_file = prochain_fichier.open(mode='rb')
            input_stream = etat_upload.fp_file

        yield chunk

        taille_uploade += len(chunk)
        etat_upload.position += len(chunk)

        if etat_upload.cb_activite:
            await etat_upload.cb_activite()


async def uploader_fichier_parts(session: aiohttp.ClientSession, etat_web: EtatWeb, fuuid,
                                 path_fichiers: pathlib.Path,
                                 batch_size=BATCH_INTAKE_UPLOAD_DEFAULT):
    ssl_context = etat_web.ssl_context
    url_consignation = await asyncio.wait_for(etat_web.get_url_consignation(), timeout=5)
    url_fichier = f'{url_consignation}/fichiers_transfert/{fuuid}'

    liste_parts = list()
    for position_part in sort_parts(path_fichiers):
        liste_parts.append(pathlib.Path(path_fichiers, '%d.part' % position_part))

    headers = {'x-fuuid': fuuid}

    stop_event = asyncio.Event()
    etat_upload = EtatUploadParts(fuuid, liste_parts, stop_event, 0)
    while not etat_upload.done:
        position = etat_upload.position
        feeder_coro = feed_filepart2(etat_upload, limit=batch_size)
        session_coro = session.put(f'{url_fichier}/{position}', ssl=ssl_context, headers=headers, data=feeder_coro)

        # Uploader chunk
        session_response = None
        try:
            session_response = await session_coro
            session_response.raise_for_status()
        finally:
            if session_response is not None:
                session_response.release()

    contenu = dict()

    with pathlib.Path(path_fichiers, ConstantesWeb.FICHIER_ETAT).open('rt') as fichier:
        contenu['etat'] = json.load(fichier)

    # Charger fichiers de transaction et cle si disponible
    try:
        with pathlib.Path(path_fichiers, ConstantesWeb.FICHIER_TRANSACTION).open('rt') as fichier:
            contenu['transaction'] = json.load(fichier)
    except OSError as e:
        if e.errno == errno.ENOENT:
            pass  # OK
        else:
            raise e

    async with session.post(url_fichier, ssl=ssl_context, headers=headers) as resp:
        resp.raise_for_status()


class IntakeJob:

    def __init__(self, fuuid: str, path_job: pathlib.Path):
        self.fuuid = fuuid
        self.path_job = path_job


class IntakeFichiers(IntakeHandler):
    """
    Gere le dechiffrage des videos.
    """

    def __init__(self, stop_event: asyncio.Event, etat: EtatWeb,
                 timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.__events_fuuids = dict()

        self.__path_intake = pathlib.Path(etat.configuration.dir_staging, ConstantesWeb.DIR_STAGING_INTAKE)
        self.__path_intake.mkdir(parents=True, exist_ok=True)

    def get_path_intake_fuuid(self, fuuid: str):
        return pathlib.Path(self.__path_intake, fuuid)

    async def run(self, stop_event: Optional[asyncio.Event] = None):
        if stop_event:
            self._stop_event = stop_event
        await asyncio.gather(
            super().run(),
            self.trigger_regulier(),
        )

    async def trigger_regulier(self):
        # Declenchement initial du traitement (recovery)
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass  # OK

        while self._stop_event.is_set() is False:
            await self.trigger_traitement()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass  # OK

    async def configurer(self):
        return await super().configurer()

    async def traiter_prochaine_job(self) -> Optional[dict]:
        try:
            repertoires = repertoires_par_date(self.__path_intake)
            path_repertoire = repertoires[0].path_fichier
            fuuid = path_repertoire.name
            repertoires = None
            self.__logger.debug("traiter_prochaine_job Traiter job intake fichier pour fuuid %s" % fuuid)
            path_repertoire.touch()  # Touch pour mettre a la fin en cas de probleme de traitement
            job = IntakeJob(fuuid, path_repertoire)
            await self.traiter_job(job)
        except IndexError:
            return None  # Condition d'arret de l'intake
        except FileNotFoundError as e:
            raise e  # Erreur fatale
        except aiohttp.ClientResponseError as e:
            raise e  # Erreur fatale cote serveur (e.g. offline)
        except Exception as e:
            self.__logger.exception("traiter_prochaine_job Erreur traitement job download")
            return {'ok': False, 'err': str(e)}

        return {'ok': True}

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def traiter_job(self, job):
        await self.handle_retries(job)

        timeout = aiohttp.ClientTimeout(connect=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            await uploader_fichier_parts(session, self._etat_instance, job.fuuid, job.path_job)

        # Supprimer le repertoire de la job
        shutil.rmtree(job.path_job)

    async def handle_retries(self, job: IntakeJob):
        path_repertoire = job.path_job
        fuuid = job.fuuid
        path_fichier_retry = pathlib.Path(path_repertoire, 'retry.json')

        # Conserver marqueur pour les retries en cas d'erreur
        try:
            with open(path_fichier_retry, 'rt') as fichier:
                info_retry = json.load(fichier)
        except FileNotFoundError:
            info_retry = {'retry': -1}

        if info_retry['retry'] > 3:
            self.__logger.error("Job %s irrecuperable, trop de retries" % fuuid)
            shutil.rmtree(path_repertoire)
            raise Exception('too many retries')
        else:
            info_retry['retry'] += 1
            with open(path_fichier_retry, 'wt') as fichier:
                json.dump(info_retry, fichier)

    async def ajouter_upload(self, path_upload: pathlib.Path):
        """ Ajoute un upload au intake. Transfere path source vers repertoire intake. """
        path_etat = pathlib.Path(path_upload, ConstantesWeb.FICHIER_ETAT)
        path_fichier_str = str(path_etat)
        self.__logger.debug("Charger fichier %s" % path_fichier_str)
        with open(path_fichier_str, 'rt') as fichier:
            etat = json.load(fichier)
        fuuid = etat['hachage']
        path_intake = self.get_path_intake_fuuid(fuuid)

        # S'assurer que le repertoire parent existe
        path_intake.parent.mkdir(parents=True, exist_ok=True)

        # Deplacer le repertoire d'upload vers repertoire intake
        self.__logger.debug("ajouter_upload Deplacer repertoire upload vers %s" % path_intake)
        try:
            path_upload.rename(path_intake)
        except OSError as e:
            if e.errno == errno.ENOTEMPTY:
                self.__logger.info("ajouter_upload Repertoire intake pour %s existe deja (OK) - supprimer upload redondant" % fuuid)
                shutil.rmtree(path_upload)
                return
            else:
                raise e

        # Declencher traitement si pas deja en cours
        await self.trigger_traitement()

    async def emettre_transactions(self, job: IntakeJob):
        producer = self._etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), 20)

        path_transaction = pathlib.Path(job.path_job, ConstantesWeb.FICHIER_TRANSACTION)
        try:
            with open(path_transaction, 'rb') as fichier:
                transaction = json.load(fichier)
        except FileNotFoundError:
            pass  # OK
        else:
            # Emettre transaction
            routage = transaction['routage']
            await producer.executer_commande(
                transaction,
                action=routage['action'], domaine=routage['domaine'], partition=routage.get('partition'),
                exchange=Constantes.SECURITE_PRIVE,
                timeout=60,
                noformat=True
            )

        path_cles = pathlib.Path(job.path_job, ConstantesWeb.FICHIER_CLES)
        try:
            with open(path_cles, 'rb') as fichier:
                cles = json.load(fichier)
        except FileNotFoundError:
            pass  # OK
        else:
            # Emettre transaction
            routage = cles['routage']
            producer.executer_commande(
                cles,
                action=routage['action'], domaine=routage['domaine'], partition=routage.get('partition'),
                exchange=Constantes.SECURITE_PRIVE,
                timeout=60,
                noformat=True
            )


class ReceptionFichiersMiddleware:
    """
    Middleware de reception de fichiers pour un module qui support l'upload par des usagers.
    """

    def __init__(self, app: aiohttp.web.Application, etat: EtatWeb, upload_url_pathname: str):
        """
        
        :param app: 
        :param etat: 
        :param upload_url_pathname: Pathname (url) de l'upload, e.g. /collections/fichiers/upload 
        """
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__app = app
        self.__etat = etat
        self.__upload_path = upload_url_pathname

        self.__stop_event: Optional[asyncio.Event] = None

        self.__connexions_write_sem = asyncio.BoundedSemaphore(3)
        self.__connexions_read_sem = asyncio.BoundedSemaphore(10)

        self.__ssl_context: Optional[SSLContext] = None

        self.__queue_verifier_parts: Optional[asyncio.Queue] = None

        # Note : stop_event est None, doit faire le wiring dans .run()
        self.__intake = IntakeFichiers(self.__stop_event, etat)

    async def setup(self):
        self._preparer_routes()
        self._charger_ssl()
        await self.__intake.configurer()

        path_staging = pathlib.Path(self.__etat.configuration.dir_staging)
        path_staging.mkdir(parents=True, exist_ok=True)

    def _preparer_routes(self):
        upload_path = self.__upload_path
        # Upload
        self.__app.add_routes([
            web.put('%s/{batch_id}/{position}' % upload_path, self.handle_put),
            web.post('%s/{batch_id}' % upload_path, self.handle_post),
            web.delete('%s/{batch_id}' % upload_path, self.handle_delete),
        ])

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__etat.configuration.cert_pem_path)
        configuration = self.__etat.configuration
        self.__ssl_context.load_cert_chain(configuration.cert_pem_path, self.__etat.configuration.key_pem_path)
        self.__ssl_context.load_verify_locations(cafile=configuration.ca_pem_path)
        self.__ssl_context.verify_mode = VerifyMode.CERT_REQUIRED

    def get_path_upload_batch(self, batch_id: str):
        configuration = self.__etat.configuration
        return pathlib.Path(configuration.dir_staging, ConstantesWeb.DIR_STAGING_UPLOAD, batch_id)

    async def handle_put(self, request: Request) -> StreamResponse:
        async with self.__connexions_write_sem:
            batch_id = request.match_info['batch_id']
            position = request.match_info['position']
            headers = request.headers

            # Afficher info (debug)
            self.__logger.debug("handle_put batch_id: %s position: %s" % (batch_id, position))
            for key, value in headers.items():
                self.__logger.debug('handle_put key: %s, value: %s' % (key, value))

            # content_hash = headers.get('x-content-hash') or headers.get('x-fuuid')
            content_hash = headers.get('x-content-hash')
            try:
                content_length = int(headers['Content-Length'])
            except KeyError:
                content_length = None

            # Creer repertoire pour sauvegader la partie de fichier
            path_upload = self.get_path_upload_batch(batch_id)
            path_upload.mkdir(parents=True, exist_ok=True)

            # S'assurer que le fichier n'existe pas deja
            try:
                path_fichier = pathlib.Path(path_upload, '%s.part' % position)
                path_fichier.stat()
                return web.HTTPConflict()
            except OSError as e:
                if e.errno == errno.ENOENT:
                    pass  # OK, le fichier n'existe pas
                else:
                    raise e

            self.__logger.debug("handle_put Conserver part %s" % path_fichier)

            if content_hash:
                verificateur = VerificateurHachage(content_hash)
            else:
                verificateur = None

            # Recevoir stream
            try:
                with open(path_fichier, 'wb', buffering=1024*1024) as fichier:
                    async for chunk in request.content.iter_chunked(64 * 1024):
                        if verificateur:
                            verificateur.update(chunk)
                        fichier.write(chunk)
            except Exception:
                self.__logger.exception("Erreur sauvegarde fichier part %s", path_fichier)
                path_fichier.unlink(missing_ok=True)
                return web.HTTPServerError()

            # Verifier hachage de la partie
            if verificateur:
                try:
                    verificateur.verify()
                except ErreurHachage as e:
                    self.__logger.info("handle_put Erreur verification hachage : %s" % str(e))
                    path_fichier.unlink(missing_ok=True)
                    return web.HTTPBadRequest()

            # Verifier que la taille sur disque correspond a la taille attendue
            # Meme si le hachage est OK, s'assurer d'avoir conserve tous les bytes
            stat = path_fichier.stat()
            if content_length is not None and stat.st_size != content_length:
                self.__logger.info("handle_put Erreur verification taille, sauvegarde %d, attendu %d" % (stat.st_size, content_length))
                path_fichier.unlink(missing_ok=True)
                return web.HTTPBadRequest()

            self.__logger.debug("handle_put batch_id: %s position: %s recu OK" % (batch_id, position))

            return web.HTTPOk()

    async def handle_post(self, request: Request) -> StreamResponse:
        async with self.__connexions_write_sem:
            batch_id = request.match_info['batch_id']
            self.__logger.debug("handle_post %s" % batch_id)

            headers = request.headers
            if request.body_exists:
                body = await request.json()
                self.__logger.debug("handle_post body\n%s" % json.dumps(body, indent=2))
            else:
                # Aucun body - transferer le contenu du fichier sans transactions (e.g. image small)
                body = None

            # Afficher info (debug)
            self.__logger.debug("handle_post fuuid: %s" % batch_id)
            for key, value in headers.items():
                self.__logger.debug('handle_post key: %s, value: %s' % (key, value))

            path_upload = self.get_path_upload_batch(batch_id)

            path_etat = pathlib.Path(path_upload, ConstantesWeb.FICHIER_ETAT)
            if body is not None:
                # Valider body, conserver json sur disque
                etat = body['etat']
                hachage = etat['hachage']
                with open(path_etat, 'wt') as fichier:
                    json.dump(etat, fichier)

                try:
                    transaction = body['transaction']
                    await self.__etat.validateur_message.verifier(transaction)  # Lance exception si echec verification
                    path_transaction = pathlib.Path(path_upload, ConstantesWeb.FICHIER_TRANSACTION)
                    with open(path_transaction, 'wt') as fichier:
                        json.dump(transaction, fichier)
                except KeyError:
                    pass

                try:
                    cles = body['cles']
                    await self.__etat.validateur_message.verifier(cles)  # Lance exception si echec verification
                    path_cles = pathlib.Path(path_upload, ConstantesWeb.FICHIER_CLES)
                    with open(path_cles, 'wt') as fichier:
                        json.dump(cles, fichier)
                except KeyError:
                    pass
            else:
                # Sauvegarder etat.json sans body
                etat = {'hachage': batch_id, 'retryCount': 0, 'created': int(datetime.datetime.utcnow().timestamp()*1000)}
                hachage = batch_id
                with open(path_etat, 'wt') as fichier:
                    json.dump(etat, fichier)

            # Valider hachage du fichier complet (parties assemblees)
            try:
                job_valider = JobVerifierParts(path_upload, hachage)
                await self.__queue_verifier_parts.put(job_valider)
                await asyncio.wait_for(job_valider.done.wait(), timeout=270)
                if job_valider.exception is not None:
                    raise job_valider.exception
            except Exception as e:
                self.__logger.warning('handle_post Erreur verification hachage fichier %s assemble : %s' % (batch_id, e))
                shutil.rmtree(path_upload)
                return web.HTTPFailedDependency()

            # Transferer vers intake
            try:
                await self.__intake.ajouter_upload(path_upload)
            except Exception as e:
                self.__logger.warning('handle_post Erreur ajout fichier %s assemble au intake : %s' % (batch_id, e))
                shutil.rmtree(path_upload)
                return web.HTTPServerError()

            return web.HTTPAccepted()

    async def handle_delete(self, request: Request) -> StreamResponse:
        async with self.__connexions_write_sem:
            batch_id = request.match_info['batch_id']

            # Afficher info (debug)
            self.__logger.debug("handle_delete %s" % batch_id)

            path_upload = self.get_path_upload_batch(batch_id)
            try:
                shutil.rmtree(path_upload)
            except FileNotFoundError:
                return web.HTTPNotFound()
            except Exception as e:
                self.__logger.info("handle_delete Erreur suppression upload %s : %s" % (batch_id, e))
                return web.HTTPServerError()

            return web.HTTPOk()

    async def thread_entretien(self):
        self.__logger.debug('Entretien web')

        while not self.__stop_event.is_set():

            # TODO Entretien uploads

            try:
                await asyncio.wait_for(self.__stop_event.wait(), 30)
            except asyncio.TimeoutError:
                pass

    async def thread_verifier_parts(self):
        self.__queue_verifier_parts = asyncio.Queue(maxsize=20)
        pending = [
            asyncio.create_task(self.__stop_event.wait()),
            asyncio.create_task(self.__queue_verifier_parts.get())
        ]
        while self.__stop_event.is_set() is False:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            # Conditions de fin de thread
            if self.__stop_event.is_set() is True:
                for p in pending:
                    p.cancel()
                    try:
                        await p
                    except asyncio.CancelledError:
                        pass  # OK
                    except AttributeError:
                        pass  # Pas une task
                for d in done:
                    if d.exception():
                        raise d.exception()
                return  # Stopped

            for t in done:
                if t.exception():
                    for p in pending:
                        try:
                            p.cancel()
                            await p
                        except asyncio.CancelledError:
                            pass  # OK
                        except AttributeError:
                            pass  # Pas une task

                    raise t.exception()

            for d in done:
                if d.exception():
                    self.__logger.error("thread_verifier_parts Erreur traitement message : %s" % d.exception())
                else:
                    job_verifier_parts: JobVerifierParts = d.result()
                    try:
                        path_upload = job_verifier_parts.path_upload
                        hachage = job_verifier_parts.hachage
                        args = [path_upload, hachage]
                        # Utiliser thread pool pour validation
                        await asyncio.to_thread(valider_hachage_upload_parts, *args)
                    except Exception as e:
                        self.__logger.exception("thread_verifier_parts Erreur verification hachage %s" % job_verifier_parts.hachage)
                        job_verifier_parts.exception = e

                    # Liberer job
                    job_verifier_parts.done.set()

            if len(pending) == 0:
                raise Exception('arrete indirectement (pending vide)')

            pending.add(asyncio.create_task(self.__queue_verifier_parts.get()))

    async def run(self, stop_event: Optional[asyncio.Event] = None):
        if stop_event is not None:
            self.__stop_event = stop_event
        else:
            self.__stop_event = asyncio.Event()

        try:
            self.__logger.info("Thread transfert fichiers demarree")
            await asyncio.gather(
                self.thread_entretien(),
                self.thread_verifier_parts(),
                self.__intake.run(self.__stop_event),
                self.__etat.charger_consignation_thread(self.__stop_event),
            )
        finally:
            self.__logger.info("Thread transfert fichiers arrete")


def valider_hachage_upload_parts(path_upload: pathlib.Path, hachage: str):
    positions = list()
    for item in path_upload.iterdir():
        if item.is_file():
            nom_fichier = str(item)
            if nom_fichier.endswith('.part'):
                position = int(item.name.split('.')[0])
                positions.append(position)
    positions = sorted(positions)

    verificateur = VerificateurHachage(hachage)

    for position in positions:
        path_fichier = pathlib.Path(path_upload, '%d.part' % position)

        with open(path_fichier, 'rb') as fichier:
            while True:
                chunk = fichier.read(64*1024)
                if not chunk:
                    break
                verificateur.update(chunk)

    verificateur.verify()  # Lance une exception si le hachage est incorrect


def extract_subject(dn: str):
    cert_subject = dict()
    for e in dn.split(','):
        key, value = e.split('=')
        cert_subject[key] = value

    return cert_subject


def get_common_name(request: Request):
    headers = request.headers
    peercert = request.get_extra_info('peercert')
    subject_info = [v[0] for v in peercert['subject']]
    cert_ou = [v[1] for v in subject_info if v[0] == 'organizationalUnitName'].pop()
    common_name = [v[1] for v in subject_info if v[0] == 'commonName'].pop()

    if cert_ou == 'nginx':
        if headers.get('VERIFIED') == 'SUCCESS':
            # Utiliser les headers fournis par nginx
            cert_subject = extract_subject(headers.get('DN'))
            common_name = cert_subject['CN']
        elif headers.get('VERIFIED') == 'INTERNAL':
            # Flag override interne - on laisse passer sans CN
            common_name = None
        else:
            raise Forbidden()
    else:
        # Connexion interne - on ne peut pas verifier que le certificat a l'exchange secure, mais la
        # connexion est directe (interne au VPN docker) et il est presentement valide.
        pass

    return common_name


class Forbidden(Exception):
    pass


class RepertoireStat:

    def __init__(self, path_fichier: pathlib.Path):
        self.path_fichier = path_fichier
        self.stat = path_fichier.stat()

    @property
    def modification_date(self) -> float:
        return self.stat.st_mtime


def repertoires_par_date(path_parent: pathlib.Path) -> list[RepertoireStat]:

    repertoires = list()
    for item in path_parent.iterdir():
        if item.is_dir():
            repertoires.append(RepertoireStat(item))

    # Trier repertoires par date
    repertoires = sorted(repertoires, key=get_modification_date)

    return repertoires


def get_modification_date(item: RepertoireStat) -> float:
    return item.modification_date


def reassembler_fichier(job: IntakeJob) -> pathlib.Path:
    path_repertoire = job.path_job
    fuuid = job.fuuid
    path_fuuid = pathlib.Path(path_repertoire, fuuid)

    if path_fuuid.exists() is True:
        # Le fichier reassemble existe deja
        return path_fuuid

    path_work = pathlib.Path(path_repertoire, '%s.work' % fuuid)
    path_work.unlink(missing_ok=True)

    parts = sort_parts(path_repertoire)
    verificateur = VerificateurHachage(fuuid)
    with open(path_work, 'wb') as output:
        for position in parts:
            path_part = pathlib.Path(path_repertoire, '%d.part' % position)
            with open(path_part, 'rb') as part_file:
                while True:
                    chunk = part_file.read(64*1024)
                    if not chunk:
                        break
                    output.write(chunk)
                    verificateur.update(chunk)

    verificateur.verify()  # Lance ErreurHachage en cas de mismatch

    # Renommer le fichier .work
    path_work.rename(path_fuuid)

    return path_fuuid


def sort_parts(path_upload: pathlib.Path):
    positions = list()
    for item in path_upload.iterdir():
        if item.is_file():
            nom_fichier = str(item)
            if nom_fichier.endswith('.part'):
                position = int(item.name.split('.')[0])
                positions.append(position)
    positions = sorted(positions)
    return positions
