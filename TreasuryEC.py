import importlib
from pathlib import Path
import os
import pandas as pd
import helper
import numpy as np
import re
from helper import setupLog


def ECdat():
       #additional mapping
       path =r"N:\Project\51448_ARPA\DC1\6. Data\ExpenditureFromTreasury"
       file = "Copy of updated_recipient_id_july_2022_mathematica_jurisdictions.xlsx"
       mapping22 = pd.read_excel(os.path.join(path, file))
       # correction for spartanburg
       #mapping22=  (mapping22['Recipient-ID'] == 'RCP-037281') & (mapping22['Level of government']=='City')


       #exclude non matched 
       #mapping22 = mapping22[mapping22["Recipient-ID"].notnull()].drop('Unnamed: 0', axis=1)
       # import modules/files
       helper= importlib.import_module('helper') # helper function class
       ip= importlib.import_module('inputData')
       # logger= helper.setupLog()
       currpath = Path.cwd()
       fpath = Path.joinpath(currpath,'config.yml')
       config = helper.import_config(fpath)
       # configinput = config['input']
       
       configinput = config['input']
       dataobj = ip.InputData(**configinput)
       dat = dataobj.read_data(group="Fund")
       varkeep = ['Level_of_Goverment', 'State', 'Jurisdication','SLFRF_Award',
       'Population','URL_I']
       dat = dat[varkeep]
      
       ##treasury data
       EConfig = config['TreasuryData']
       EC = ip.InputData(**EConfig)
       ECdat = EC.readTreasury()

       
       #EC Project sum to get expenditure total
       ECPall = pd.DataFrame()
       for i in ['EC22']:
              ECP= ECdat[i]['Projects']
              ECP = ECP[ ECP['ReportingTier'].str.startswith('Tier 1')]
              if (i=="EC22"):
                     ECP = ECP.join(mapping22.set_index("Recipient-ID"), on="Recipient_ID", how="inner")
              ECP['Year']= int('20'+i[2:])
              ECPall= ECPall.append(ECP)


       dropcol = ['Project_ID','AdoptedBudget','Cumulative_Obligations']
       ECPall = ECPall.drop(dropcol, axis=1).rename(columns={'ExpenditureCategoryGroup':'variable',
       'Cumulative_Expenditures':'value'})
       #ECPall[["State or territory","Jurisdiction"]].drop_duplicates().shape
       #take out ExpenditureCategory
       ECPall.columns
       ECdatPnoProj = ECPall.groupby(['Recipient_ID','RecipientName', 'State', 'ReportingTier',
              'RecipientType', 'variable','Year',
               'Level of government', 'State or territory', 'Jurisdiction'])['value'].sum().reset_index().drop('State', axis=1).rename(
                     columns={'Level of government':'Level_of_Goverment', 'State or territory':'State',
                      'Jurisdiction':'Jurisdication'})
       
       
       ECdatPagg = ECPall.groupby(['Level of government', 'State or territory', 'Jurisdiction','Year'])['value'].sum()
       ECdatPagg= ECdatPagg.reset_index()

      
       
       #ECR[["State or territory","Jurisdiction","Level of government"]].drop_duplicates().shape
       
       #add back the total expenditure
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
       #ECRL[["State or territory","Jurisdiction","Level of government"]].drop_duplicates().shape

       #append both by var name
       ECRL.columns
       ECdatPnoProj.columns
       #ECdatPnoProj = ECdatPnoProj
       EC = ECRL.append(ECdatPnoProj).drop(['Recipient_ID','RecipientName','ReportingTier','RecipientType'], axis=1)
       EC.isnull().sum()
  
       #add plan URL for 2022
       # EC= EC.join(ECdat['EC22']['Plan'].set_index('Recipient_ID'), on='Recipient_ID') #add URL
       #EC.query('RecipientName=="County Of Monroe, New York"')
       #add SFLR in columns
       ECR.columns
       var= [ 'Level_of_Goverment', 'State', 'Jurisdication','Year']
       EC = EC.drop(['Population','URL_I'], axis=1).join(ECR.set_index(var), on=var)
       #delete those without SLFRF awards for now
       EC.isnull().sum()
       EC['Year']= EC['Year'].fillna(2022)
      
       EC['SpendPct']=EC['value']/EC['Expended_Funds']
       ECSP = EC[EC['variable'].isin(['SLFRF_Award', 'Expended_Funds', 'AvailableFund'])==False]
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

       ECSP = ECSP.join(National.set_index(['Year','variable']), on=['Year','variable'], rsuffix='_natl')
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


