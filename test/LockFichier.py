from millegrilles_messages.FileLocking import FileLock

PATH_FICHIER_1 = '/tmp/process1.lock'
PATH_FICHIER_2 = '/tmp/process2.lock'


def fichier1():
    with FileLock(PATH_FICHIER_1):
        pass

    FileLock(PATH_FICHIER_2, lock_timeout=10).__enter__()



def main():
    fichier1()


if __name__ == '__main__':
    main()
