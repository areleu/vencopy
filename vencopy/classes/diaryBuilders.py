__version__ = '0.4.X'
__maintainer__ = 'Niklas Wulff'
__contributors__ = 'Fabia Miorelli'
__email__ = 'Niklas.Wulff@dlr.de'
__birthdate__ = '21.04.2019'
__status__ = 'dev'  # options are: dev, test, prod
__license__ = 'BSD-3-Clause'

if __package__ is None or __package__ == '':
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.dirname(__file__))))


class DiaryBuilder:
    def __init__(
        self,
        configDict: dict
    ):
        pass


if __name__ == '__main__':
    # from vencopy.classes.dataParsers import ParseMiD, ParseKiD, ParseVF
    # from vencopy.scripts.globalFunctions import loadConfigDict
    pass
