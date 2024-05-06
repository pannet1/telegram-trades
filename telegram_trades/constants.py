from toolkit.fileutils import Fileutils
from toolkit.logger import Logger
from toolkit.utilities import Utilities

DATA = "../data/"
logging = Logger(20, DATA + "file.log")
F_TASK = DATA + "tasks.json"
DIRP = "../../"
FUTL = Fileutils()
SETG = FUTL.get_lst_fm_yml(DIRP + "ravikanth.yml")
BRKR = SETG["aliceblue"]
logging.debug(BRKR)
TGRM = SETG["telegram"]
logging.debug(TGRM)
UTIL = Utilities()
CHANNEL_DETAILS = SETG["channel_details"]
