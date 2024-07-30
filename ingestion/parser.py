

import numpy as np
import pandas as pd

class objectList:
    def __init__(self, objList):
        self.content = objList

    def dfParse(self):
        df = pd.DataFrame([vars(obj) for obj in self.content])
        return df