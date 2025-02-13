from toolkit.fileutils import Fileutils
from toolkit.logger import Logger
from toolkit.utilities import Utilities

DATA = "../data/"
S_LOG = DATA + "file.log"

F_TASK = DATA + "tasks.json"
DIRP = "../../"
FUTL = Fileutils()


if not FUTL.is_file_exists(S_LOG):
    """
    description:
        create data dir and log file
        if did not if file did not exists
    input:
         file name with full path
    """
    print("creating data dir")
    FUTL.add_path(S_LOG)
elif FUTL.is_file_not_2day(S_LOG):
    FUTL.nuke_file(S_LOG)

logging = Logger(20, S_LOG)
SETG = FUTL.get_lst_fm_yml(DIRP + "ravikanth.yml")
BRKR = SETG["aliceblue"]
logging.debug(BRKR)
TGRM = SETG["telegram"]
logging.debug(TGRM)
UTIL = Utilities()
CHANNEL_DETAILS = SETG["channel_details"]
STRIKE_PRICE_DIFF = SETG["strike_price_difference"]
