import importlib
from pathlib import Path
import os
import pandas as pd
import helper
import numpy as np
import re


# import modules/files
helper= importlib.import_module('helper') # helper function class
ip= importlib.import_module('inputData')
logger= helper.setupLog()
currpath = Path.cwd()
fpath = Path.joinpath(currpath,'config.yml')
config = helper.import_config(fpath)
configinput = config['input']

##treasury data
#2022
ECdatConfig = config['TreasuryData22']
EC = ip.InputData(**ECdatConfig)
ECdat22 = EC.readTreasury()

#2021
ECdatConfig1 = config['TreasuryData21']
EC1 = ip.InputData(**ECdatConfig1)
ECdat21 = EC1.readTreasury()   

#EC Project sum to get expenditure total
#2022
#EC project
ECdat22P = ECdat22['Projects']
ECdat22P = ECdat22P[ ECdat22P['ReportingTier'].str.startswith('Tier 1')]
ECdat22P['Year']=2022

#2011
#ECProject
ECdat21P = ECdat21['Projects']
ECdat21P = ECdat21P[ ECdat21P['ReportingTier'].str.startswith('Tier 1')]
ECdat21P['Year']=2021

ECdatP = ECdat22P.append(ECdat21P)
ECdatP.isnull().sum()

dropcol = ['Project_ID','AdoptedBudget','Cumulative_Obligations']
ECdatP = ECdatP.drop(dropcol, axis=1).rename(columns={'ExpenditureCategoryGroup':'variable',
 'Cumulative_Expenditures':'value'})
ECdatP.columns
ECdatPnoProj = ECdatP.groupby(['Recipient_ID','RecipientName', 'State', 'ReportingTier',
       'RecipientType', 'variable', 'ExpenditureCategory','Year'])['value'].sum().reset_index()


ECdatPagg = ECdatP.groupby(['Recipient_ID','Year'])['value'].sum()

#recipient data 
#2022
ECdat22R = ECdat22['Recipients']
ECdat22R = ECdat22R[ECdat22R['ReportingTier'].str.startswith('Tier 1')]
ECdat22R.columns
#2021
ECdat21R = ECdat21['Recipients']
ECdat21R = ECdat21R[ECdat21R['ReportingTier'].str.startswith('Tier 1')]
ECdat21R.columns
ECdat21R= ECdat21R.join(ECdat22R.drop(['Recipient_ID', 'ReportingTier', 'RecipientType','Num_Projects'], axis=1).\
    set_index(['RecipientName', 'State']), on=['RecipientName', 'State'], how='inner')
ECdat21R['Year']=2021
ECdat22R['Year']=2022
ECR = ECdat22R.append(ECdat21R)
#add expended
ECR.isnull().sum()
ECR= ECR.join(ECdatPagg, on =['Recipient_ID','Year'])
ECR =ECR.rename(columns={'value':'Expended_Funds'})
ECR['AvailableFund']= ECR['SLFRF_Award']- ECR['Expended_Funds'].fillna(0) #fill missing for now. might need to remove with prod data
keyvar = [x for x in ECR.columns if not x in ['SLFRF_Award','Expended_Funds','AvailableFund']]
keyvar
ECRL = ECR.melt(id_vars= keyvar).drop(['Num_Projects'], axis=1)
ECRL.columns
ECdatP.columns
#append both by var name
EC = ECRL.append(ECdatPnoProj)

#add plan URL for 2022
EC= EC.join(ECdat22['Plan'].set_index('Recipient_ID'), on='Recipient_ID') #add URL
#add SFLR in columns
var= ['Recipient_ID', 'RecipientName', 'State', 'ReportingTier',
       'RecipientType', 'Year']
EC = EC.join(ECR.set_index(var), on=var)
#delete those without SLFRF awards for now
EC = EC.query("SLFRF_Award==SLFRF_Award")
EC.isnull().sum()
groupbyvar = [x for x in EC.columns if not x in ("ExpenditureCategory" ,"value")]
ECt = EC.query("ExpenditureCategory==ExpenditureCategory").groupby(groupbyvar)['value'].sum().reset_index()
ECt['ExpenditureCategory']= 'ALL'

#sanity check
t= ECt[ ECt['variable']=="6-Revenue Replacement"]
t['pct']=t['value']/t['Expended_Funds']
t['pct'].median()
((t['Expended_Funds']>0) & (t['value']/t['Expended_Funds']<0.01) ).sum()
t.columns

EC = EC.append(ECt)

helper.output_to_excel(config['output']['path'], EC,'TreasuryEC.xlsx' )

#check
EC.groupby(['Year','variable'])['value'].sum()
t=EC[(EC['Year']==2022) & (EC['variable']=='1-Public Health')].groupby('Recipient_ID')['value'].sum().reset_index()
(t['value']==0).sum()
t1 = EC[(EC['Year']==2022) & (EC['State']=='Colorado') & (EC['variable']=='1-Public Health')]