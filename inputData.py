import yaml
from pathlib import Path
# import helper
import os
import pandas as pd 
import numpy as np
import time 


from helper import read_link, setupLog


# warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)
# ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# logger.addHandler(ch)
TIME_NOW = time.strftime("%d%b%Y-%H%M%S")


class InputData(object):
  """
  for importing and processing input excel data files
  a method for each input data source
  type : string
   
  """
  def __init__(self,  **kwargs):
      for k, v in kwargs.items():
          setattr(self, k, v)

  def read_data(self, group):
    """
    group: data group
    """
    fp = self.data_path
    fn = self.fn
    filepath = os.path.join( fp, fn)
    sheet= self.var[group]['sheet']
    usecols = self.var[group]['colpos']
    srow = self.var[group]['skiprow']
    sfooter = self.var[group]['skipfooter']
    colname = self.var[group]['colnames']
    rawdf =  pd.read_excel(filepath, header=None, sheet_name=sheet,
    usecols=usecols, skiprows=srow, skipfooter=sfooter, names=colname)
    setupLog().info("{} raw dataframe has {} rows".format(group, len(rawdf)))
    if group == 'InvestmentArea':
      urlpos =self.var[group]['urllink']
      urldf = read_link(filepath, sheet, srow+1, rawdf.shape[0]+srow+1, urlpos)
      rawdf= rawdf.join(urldf)
    if group == 'Fund':
      urlposL =self.var[group]['urllink']
      for i in urlposL:
        urlpos= i
        urldf = read_link(filepath, sheet, srow+1, rawdf.shape[0]+srow+1, urlpos).drop('rowid', axis=1)
        urldf.columns= ["URL_"+urlpos]
        rawdf= rawdf.join(urldf, rsuffix=urlpos)
    return rawdf