from toolkit.fileutils import Fileutils 


DIRP = "../../" 
SETG = Fileutils().get_lst_fm_yml(DIRP + "ravikanth.yml")
BRKR = SETG["aliceblue"]
print(BRKR)
TGRM = SETG["telegram"]
print(TGRM)
