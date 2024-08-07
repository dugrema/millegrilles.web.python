import asyncio
import datetime
import errno
import json
import logging
import os
import pathlib
import shutil

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrille
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.FileLocking import FileLock, FileLockedException, is_locked

from millegrilles_web import Constantes

LOGGER = logging.getLogger(__name__)

# CONST_MAX_RETRIES_CLE = 2

CONST_INTAKE_LOCK_NAME = 'intake.lock'


class IntakeJob:

    def __init__(self, path_job: pathlib.Path):
        self.path_job = path_job


class IntakeFuuidJob(IntakeJob):

    def __init__(self, fuuid: str, path_job: pathlib.Path):
        super().__init__(path_job)
        self.fuuid = fuuid


class IntakeFichiers(IntakeHandler):
    """
    Recoit fichiers et transfere vers consignation.
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance, timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__consignation_handler = None  # TODO

        self.__events_fuuids = dict()

        self.__path_intake = pathlib.Path(self._etat_instance.configuration.dir_staging, Constantes.DIR_STAGING_INTAKE)
        self.__path_intake.mkdir(parents=True, exist_ok=True)

    def get_path_intake_fuuid(self, fuuid: str):
        return pathlib.Path(self.__path_intake, fuuid)

    async def run(self):
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
            path_lock = pathlib.Path(path_repertoire, CONST_INTAKE_LOCK_NAME)
            with FileLock(path_lock, lock_timeout=300):
                path_repertoire.touch()  # Touch pour mettre a la fin en cas de probleme de traitement
                job = IntakeJob(fuuid, path_repertoire)
                await self.traiter_job(job)
        except IndexError:
            return None  # Condition d'arret de l'intake
        except FileLockedException:
            return {'ok': False, 'err': 'job locked - traitement en cours'}
        except FileNotFoundError as e:
            raise e  # Erreur fatale
        except Exception as e:
            self.__logger.exception("traiter_prochaine_job Erreur traitement job download")
            return {'ok': False, 'err': str(e)}

        return {'ok': True}

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def traiter_job(self, job):
        await self.handle_retries(job)

        # Reassembler et valider le fichier
        args = [job]
        path_fichier = await asyncio.to_thread(reassembler_fichier, *args)

        # Consigner : deplacer fichier vers repertoire final
        await self.__consignation_handler.consigner(path_fichier, job.fuuid)

        # Charger transactions, cles. Emettre.
        await self.emettre_transactions(job)

        if self._etat_instance.est_primaire is False:
            self.__logger.debug("traiter_job Fichier consigne sur secondaire - s'assurer que le fichier existe sur primaire")
            await self.__consignation_handler.ajouter_upload_secondaire(job.fuuid)

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
        path_etat = pathlib.Path(path_upload, Constantes.FICHIER_ETAT)
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

        path_transaction = pathlib.Path(job.path_job, Constantes.FICHIER_TRANSACTION)
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
                exchange=ConstantesMillegrille.SECURITE_PRIVE,
                timeout=60,
                noformat=True
            )

        path_cles = pathlib.Path(job.path_job, Constantes.FICHIER_CLES)
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
                exchange=ConstantesMillegrille.SECURITE_PRIVE,
                timeout=60,
                noformat=True
            )


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
            # Verifier si on a un lockfile non expire
            path_lock = pathlib.Path(item, CONST_INTAKE_LOCK_NAME)
            if is_locked(path_lock, timeout=300) is False:
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
