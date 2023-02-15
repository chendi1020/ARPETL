import yaml
from pathlib import Path
# import helper
import os
import pandas as pd 
import numpy as np
import time 


from helper import read_link, setupLog, sort_jurisidiction


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
      path = self.TreasuryData['files']['EC22']['path']
      file = self.TreasuryData['files']['EC22']['fn']
      # path =r"N:\Project\51448_ARPA\DC1\6. Data\ExpenditureFromTreasury"
      # file = "July-2022-Quarterly-Reporting-Data-through-June-30-2022.xlsx"
      TProj22 = pd.read_excel(os.path.join(path, file), sheet_name="Projects")
      var= ['Project Name','Project ID','Project Description','Completion Status']
      rawdf= rawdf.join(TProj22[var].set_index('Project ID'), on='Project_ID')
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
    definvest = pd.read_excel(self.InvestmentAreaDef)
    orgrow = len(dfl)
    dfl = dfl.join(definvest.set_index('InvestmentVar'), on='variable', rsuffix='_ref')
    assert len(dfl)==orgrow
    return sort_jurisidiction(dfl)

  def readold(self):
    fp = self.data21['data_path']
    fn = self.data21['fn']
    filepath = os.path.join( fp, fn)
    sheet= self.data21['Ranking']['sheet']
    usecols = self.data21['Ranking']['colpos']
    srow = self.data21['Ranking']['skiprow']
    sfooter = self.data21['Ranking']['skipfooter']
    colname = self.data21['Ranking']['colnames']
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
    fp = self.TreasuryData['files']['EC22']['path']
    fn = self.TreasuryData['files']['EC22']['fn']
    filepath = os.path.join( fp, fn)
    tabs = self.TreasuryData['files']['EC22']['var'].keys()
    dfdict ={}
    for k in tabs:
      sheet= self.TreasuryData['files']['EC22']['var'][k]['sheet']
      usecols = self.TreasuryData['files']['EC22']['var'][k]['colpos']
      srow = self.TreasuryData['files']['EC22']['var'][k]['skiprow']
      sfooter = self.TreasuryData['files']['EC22']['var'][k]['skipfooter']
      colname = self.TreasuryData['files']['EC22']['var'][k]['colnames']
      rawdf =  pd.read_excel(filepath, header=None, sheet_name=sheet,
        usecols=usecols, skiprows=srow, skipfooter=sfooter, names=colname)
      dfdict[k]= rawdf
    return dfdict

  def ECdat(self):
       #additional mapping
       path =self.TreasuryData['files']['EC22mapping']['path']
       file = self.TreasuryData['files']['EC22mapping']['fn']
       mapping22 = pd.read_excel(os.path.join(path, file))
       dat = self.read_data(group="Fund")
       varkeep = ['Level_of_Goverment', 'State', 'Jurisdication','SLFRF_Award',
       'Population','URL_I']
       dat = dat[varkeep]
      
       ##treasury data
       ECdat = self.readTreasury()

       #EC Project sum to get expenditure total
  
       ECP= ECdat['Projects']
       ECP = ECP[ ECP['ReportingTier'].str.startswith('Tier 1')]
       ECP = ECP.join(mapping22.set_index("Recipient-ID"), on="Recipient_ID", how="inner")
       ECP['Year']= 2022
       ECPall= ECP


       dropcol = ['Project_ID','AdoptedBudget','Cumulative_Obligations']
       ECPall = ECPall.drop(dropcol, axis=1).rename(columns={'ExpenditureCategoryGroup':'variable',
       'Cumulative_Expenditures':'value'})

       ECPall.columns
       ECdatPnoProj = ECPall.groupby(['Recipient_ID','RecipientName', 'State', 'ReportingTier',
              'RecipientType', 'variable','Year',
               'Level of government', 'State or territory', 'Jurisdiction'])['value'].sum().reset_index().drop('State', axis=1).rename(
                     columns={'Level of government':'Level_of_Goverment', 'State or territory':'State',
                      'Jurisdiction':'Jurisdication'})
       
       
       ECdatPagg = ECPall.groupby(['Level of government', 'State or territory', 'Jurisdiction','Year'])['value'].sum()
       ECdatPagg= ECdatPagg.reset_index()

       ECR = dat.join(ECdatPagg.set_index(['Level of government', 'State or territory', 'Jurisdiction']),
       on=['Level_of_Goverment', 'State', 'Jurisdication'])
       t=ECR[ECR['value'].isnull()][['Jurisdication','State']]
       setupLog().info(" {} do not have project data in treasury".format(t))
       ECR['value']= ECR['value'].fillna(0) #fill expenditure to be zero
       ECR =ECR.rename(columns={'value':'Expended_Funds'})
       
       ECR['AvailableFund']= ECR['SLFRF_Award']- ECR['Expended_Funds'].fillna(0) #fill missing for now. might need to remove with prod data
       keyvar = [x for x in ECR.columns if not x in ['SLFRF_Award','Expended_Funds','AvailableFund']]
       keyvar
       ECRL = ECR.melt(id_vars= keyvar)

       #append both by var name
       ECRL.columns
       ECdatPnoProj.columns
       #ECdatPnoProj = ECdatPnoProj
       EC = ECRL.append(ECdatPnoProj).drop(['Recipient_ID','RecipientName','ReportingTier','RecipientType'], axis=1)
       EC.isnull().sum()
  
       ECR.columns
       var= [ 'Level_of_Goverment', 'State', 'Jurisdication','Year']
       EC = EC.drop(['Population','URL_I'], axis=1).join(ECR.set_index(var), on=var)
       #delete those without SLFRF awards for now
       EC.isnull().sum()
       EC['Year']= EC['Year'].fillna(2022)
      
       EC['SpendPct']=EC['value']/EC['Expended_Funds']
       ECSP = EC[EC['variable'].isin(['SLFRF_Award', 'Expended_Funds', 'AvailableFund'])==False]
       #add zero
       indexvar = [x for x in ECSP.columns if x not in ['variable','value']]
       ECSP1=ECSP.pivot_table(index= indexvar, columns='variable', values='value', aggfunc='max')
       ECSP1= ECSP1.fillna(0).reset_index()
       ECSP2 = ECSP1.melt(id_vars=indexvar)
       ECother = EC[EC['variable'].isin(['SLFRF_Award', 'Expended_Funds', 'AvailableFund'])]
       
       National = ECSP.groupby(['Year','variable']).agg({'value':sum}).reset_index()
       National['Total']= National.groupby('Year')['value'].transform(sum)
       National['SpendPct']= National['value']/National['Total']
       National = National[['Year','variable','SpendPct']]

       LvlGov = ECSP.groupby(['Level_of_Goverment','Year','variable']).agg({'value':sum}).reset_index()
       LvlGov['Total']= LvlGov.groupby(['Level_of_Goverment','Year'])['value'].transform(sum)
       LvlGov['SpendPct']= LvlGov['value']/LvlGov['Total']
       LvlGov = LvlGov[['Level_of_Goverment','Year','variable','SpendPct']]
   
       #National=ECSP[ECSP['value']>0].groupby(['Year','variable'])['SpendPct'].median().reset_index()
       #LvlGov=ECSP[ECSP['value']>0].groupby(['Level_of_Goverment','Year','variable'])['SpendPct'].median().reset_index()
       aggMedian= LvlGov.append(National).fillna('National')
       aggMedian['AggGroup']='Y'

       ECSP = ECSP2.join(National.set_index(['Year','variable']), on=['Year','variable'], rsuffix='_natl')
       ECSP = ECSP.join(LvlGov.set_index(['Year','variable','Level_of_Goverment']), on=['Year','variable','Level_of_Goverment'], rsuffix='_gov')
       
       indvar = [x for x in ECSP.columns if x not in ('SpendPct', 'SpendPct_natl','SpendPct_gov')]
       indvar
       ECSPL = ECSP.melt(id_vars= indvar, value_name='SPvalue', var_name="compgrp")
       ECSPL.isnull().sum()
       aggMedian['compgrp']='SpendPct'
       aggMedian= aggMedian.rename(columns={'SpendPct':'SPvalue'})
       EC= ECSPL.append(aggMedian)
       EC['AggGroup']=EC['AggGroup'].fillna('N')
       
       EC = EC.append(ECother)
       EC['compgrp']= np.where(EC['compgrp'].isnull(), EC['variable'], EC['compgrp'])
       EC['SPvalue']= np.where(EC['SPvalue'].isnull(), EC['value'], EC['SPvalue'])
     
       return EC




