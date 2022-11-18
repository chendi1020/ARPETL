import yaml
from pathlib import Path
# import helper
import os
import pandas as pd 
import numpy as np
import time 


from helper import read_link, setupLog, sort_jurisidiction


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
    # if group == 'InvestmentArea':
    #   urlpos =self.var[group]['urllink']
    #   urldf = read_link(filepath, sheet, srow+1, rawdf.shape[0]+srow+1, urlpos)
    #   rawdf= rawdf.join(urldf)
    if group == 'Fund':
      urlposL =self.var[group]['urllink']
      for i in urlposL:
        urlpos= i
        urldf = read_link(filepath, sheet, srow+1, rawdf.shape[0]+srow+1, urlpos).drop('rowid', axis=1)
        urldf.columns= ["URL_"+urlpos]
        rawdf= rawdf.join(urldf, rsuffix=urlpos)
    if group =="PlanInvest":
      for i in [x for x in colname if x[:3]=="PIA"]:
        rawdf[i]= np.where(rawdf[i].isnull(), "U",
        np.where(rawdf[i]=="No", "N","Y" ))
    return rawdf

  def process_plan_invest(self, df):
    indexvar = [x for x in df.columns if x[:3] != "PIA"]
    dfl = df.melt(id_vars= indexvar)
    dfl['InvestmentAreaDescr']= dfl['variable'].transform(lambda x:  self.investArea[x])
    dfl = dfl.join(df.set_index(['Level_of_Goverment','State','Jurisdication']), on=['Level_of_Goverment','State','Jurisdication'])
    return sort_jurisidiction(dfl)

  def readold(self):
    fp = self.data_path
    fn = self.fn
    filepath = os.path.join( fp, fn)
    sheet= self.Ranking['sheet']
    usecols = self.Ranking['colpos']
    srow = self.Ranking['skiprow']
    sfooter = self.Ranking['skipfooter']
    colname = self.Ranking['colnames']
    rawdf =  pd.read_excel(filepath, header=None, sheet_name=sheet,
    usecols=usecols, skiprows=srow, skipfooter=sfooter, names=colname)
    rawdf['State']= rawdf['State'].str.strip()
    setupLog().info("{} raw dataframe has {} rows".format("old rank", len(rawdf)))
    df= sort_jurisidiction(rawdf)
    df= df.query("EnhanceDataEvidenceScore_2021==EnhanceDataEvidenceScore_2021")
    list = [x for x in df.columns if "Score" in x and x != 'Score_2021']
    dictmap ={
      'No Evidence':0,
      'Clear Evidence':2,
      'Promising':1
    }
    for i in list:
      df[i]= df[i].transform(lambda x: dictmap[x] )
    return df

  def readTreasury(self):
    fp = self.path
    fn = self.fn
    filepath = os.path.join( fp, fn)
    tabs = self.var.keys()
    dfdict ={}
    for i in tabs:
      sheet= self.var[i]['sheet']
      usecols = self.var[i]['colpos']
      srow = self.var[i]['skiprow']
      sfooter = self.var[i]['skipfooter']
      colname = self.var[i]['colnames']
      rawdf =  pd.read_excel(filepath, header=None, sheet_name=sheet,
      usecols=usecols, skiprows=srow, skipfooter=sfooter, names=colname)
      dfdict[i]= rawdf
    return dfdict

