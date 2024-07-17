from setuptools import setup, find_packages
import subprocess
import os


def get_version():

    try:
        # Utiliser variable d'environnement de build (jenkins)
        version = os.environ['VBUILD']
    except KeyError:
        try:
            with open('version.txt', 'r') as version_file:
                version = version_file.readline().strip()
        except FileNotFoundError:
            with open('build.txt', "r") as buildno_file:
                build_no = buildno_file.read().strip()

            # commande_git_version = ['git', 'name-rev', '--name-only', 'HEAD']
            commande_git_version = ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
            output_process = subprocess.run(commande_git_version, stdout=subprocess.PIPE)
            version = output_process.stdout.decode('utf8').strip()
            version = '%s.%s' % (version, build_no)
            print("Version: %s" % (version))

    return version

setup(
    name='millegrilles_web_python',
    version='%s' % get_version(),
    packages=find_packages(),
    url='https://github.com/dugrema/millegrilles.web.python',
    license='AFFERO',
    author='Mathieu Dugre',
    author_email='mathieu.dugre@mdugre.info',
    description='Base web pour les applictions MilleGrilles avec client',
    install_requires=[
        'pytz>=2020.4',
        'aiohttp>=3.8.1,<4',
        'requests>=2.28.1,<3',
        'pyjwt',
        'python-socketio>=5.9,<6',
        'aioredis',
        'aiohttp-session[aioredis]'
    ]
)
