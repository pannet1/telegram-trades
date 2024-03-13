from toolkit.fileutils import Fileutils
from toolkit.logger import Logger
from toolkit.utilities import Utilities

DATA = "../data/"
logging = Logger(20, DATA + "file.log")
DIRP = "../../"
FUTL = Fileutils()
SETG = FUTL.get_lst_fm_yml(DIRP + "ravikanth.yml")
BRKR = SETG["aliceblue"]
logging.debug(BRKR)
TGRM = SETG["telegram"]
logging.debug(TGRM)
UTIL = Utilities()
