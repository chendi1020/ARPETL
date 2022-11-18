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
# configinput = config['input']

##treasury data
EConfig = config['TreasuryData']
EC = ip.InputData(**EConfig)
ECdat = EC.readTreasury()


#EC Project sum to get expenditure total
ECPall = pd.DataFrame()
for i in ECdat.keys():
       ECP= ECdat[i]['Projects']
       ECP = ECP[ ECP['ReportingTier'].str.startswith('Tier 1')]
       ECP['Year']= int('20'+i[2:])
       ECPall= ECPall.append(ECP)

ECPall.columns

dropcol = ['Project_ID','AdoptedBudget','Cumulative_Obligations']
ECPall = ECPall.drop(dropcol, axis=1).rename(columns={'ExpenditureCategoryGroup':'variable',
 'Cumulative_Expenditures':'value'})
ECdatPnoProj = ECPall.groupby(['Recipient_ID','RecipientName', 'State', 'ReportingTier',
       'RecipientType', 'variable', 'ExpenditureCategory','Year'])['value'].sum().reset_index()
#total expenditure by jurisdiction
ECdatPagg = ECPall.groupby(['RecipientName', 'State','Year'])['value'].sum()

#recipient data 
ECRecpall = pd.DataFrame()
for i in ECdat.keys():
       ECR= ECdat[i]['Recipients']
       ECR = ECR[ ECR['ReportingTier'].str.startswith('Tier 1')]
       ECR['Year']= int('20'+i[2:])
       ECRecpall= ECRecpall.append(ECR)

ECRecpall.isnull().sum()
ECRecpall.shape
keyvar =['RecipientName','State','ReportingTier','RecipientType','Year']
ECRecpallRv = ECRecpall[keyvar].drop_duplicates() #drop recipient ID
SLFRF = ECRecpall[ECRecpall['Year']==2022]
ECRecpallRv=ECRecpallRv.join(SLFRF[['RecipientName', 'State','SLFRF_Award']].set_index(['RecipientName', 'State']),
on=['RecipientName', 'State'])
ECRecpallRv.isnull().sum()
ECR = ECRecpallRv.query("SLFRF_Award==SLFRF_Award")

ECR= ECR.join(ECdatPagg, on =['RecipientName', 'State','Year'])
ECR =ECR.rename(columns={'value':'Expended_Funds'})
ECR['AvailableFund']= ECR['SLFRF_Award']- ECR['Expended_Funds'].fillna(0) #fill missing for now. might need to remove with prod data
keyvar = [x for x in ECR.columns if not x in ['SLFRF_Award','Expended_Funds','AvailableFund']]
keyvar
ECRL = ECR.melt(id_vars= keyvar)

#append both by var name
EC = ECRL.append(ECdatPnoProj)
EC.isnull().sum()
#add plan URL for 2022
EC= EC.join(ECdat['EC22']['Plan'].set_index('Recipient_ID'), on='Recipient_ID') #add URL
EC.shape
#add SFLR in columns
var= [ 'RecipientName', 'State', 'ReportingTier', 'RecipientType', 'Year']
EC = EC.join(ECR.set_index(var), on=var)
#delete those without SLFRF awards for now
EC = EC.query("SLFRF_Award==SLFRF_Award")

groupbyvar = [x for x in EC.columns if not x in ("ExpenditureCategory" ,"value")]
ECt = EC.query("ExpenditureCategory==ExpenditureCategory").groupby(groupbyvar)['value'].sum().reset_index()
ECt['ExpenditureCategory']= 'ALL'
EC = EC.append(ECt)
#sanity check
t= ECt[ ECt['variable']=="6-Revenue Replacement"]
t['pct']=t['value']/t['Expended_Funds']
t['pct'].median()
# ((t['Expended_Funds']>0) & (t['value']/t['Expended_Funds']<0.01) ).sum()
# t.columns

helper.output_to_excel(config['output']['path'], EC,'TreasuryEC.xlsx' )

