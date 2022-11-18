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

dataobj = ip.InputData(**configinput)
logger.info("start reading input data")

#plan Investment
pinvest = dataobj.read_data("PlanInvest")
pinvestL = dataobj.process_plan_invest(pinvest)

#dat is fund
dat = dataobj.read_data(group="Fund")
numlist = ['SLFRF_Award','Population', 'Award_Per_Capita', 'Num_Projects']+[x for x in dat.columns if "Score" in x]
for i in numlist:
    dat[i]= pd.to_numeric(dat[i], errors='coerce')
dat['Score_2022']= dat[[x for x in dat.columns if "Score" in x]].fillna(0).sum(axis=1)
dat = helper.sort_jurisidiction(dat)
dat.columns
dat.shape
#sort data
dat = helper.sort_jurisidiction(dat)
logger.info("Fund dataframe "+ helper.check_STAbbr(dat)) 
#old ranking
oldconfig = config['data21']
olddata = ip.InputData(**oldconfig)
oldrank = olddata.readold()
oldrank.columns

rank = dat .join(oldrank.set_index(['Jurisdiction','STAbbr','Level_of_Goverment']).drop(['Jurisdication','State'], axis=1),
on = ['Jurisdiction','STAbbr','Level_of_Goverment'] )
indexvar = [x for x in rank.columns if not re.search(r'\d+$',x)]
indexvar
rankL = rank.melt(id_vars= indexvar)
rankL.columns
rankL['Year']= rankL['variable'].str[-4:]
rankL= rankL.query("value==value")
rankL['Group']= rankL['variable'].str.replace("_\d+","")
rankL1 =rankL[rankL['Group'].isin([x for x in rankL['Group'].unique().tolist() if "Score" in x ])].\
    pivot_table(index=['Jurisdiction','Group'], columns='Year', values='value', aggfunc=max).reset_index()
rankL1['Change']= np.where( (rankL1['2022'].isnull() ) | (rankL1['2021'].isnull()), 'U', 
np.where(rankL1['2022']>rankL1['2021'], 'Up',
np.where( rankL1['2022']==rankL1['2021'],'Same','Down') ) )
rankL= rankL.join(rankL1.set_index(['Jurisdiction','Group']), on=['Jurisdiction','Group'])

##investment area and coding
invest = dataobj.read_data(group="InvestmentArea")
invest = helper.sort_jurisidiction(invest)
helper.check_STAbbr(invest)
invest.columns
#convert to numeric
numlist = ['ActivityFund','EvidenceBasedAmount','ImpactEvaluationAmount','DataEvidenceAmount']
for i in numlist:
    invest[i]= pd.to_numeric(invest[i], errors='coerce')

invest['Activity'].unique()



sr = config['input']['sr']
#drop sr transpose available fund
dat['AvailableFund']= dat['SLFRF_Award']- dat['Expended_Funds'].fillna(0) #fill missing for now. might need to remove with prod data
keyvar = ['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication','Jurisdiction']
datL = dat.drop(sr+['RecoveryPlan_2021', 'RecoveryPlan_Interim', 'RecoveryPlan_2022',
'URL_G', 'URL_H', 'URL_I','Num_Projects','Expenditure_EC34'], axis=1).melt(id_vars= keyvar+['Population'])
datL['variable'].unique()
datL=datL.join(dat.set_index(keyvar+['Population']), on =keyvar+['Population'])


#strength of response melt
leftvar = [x for x in dat.columns if x not in sr and x not in keyvar ]
srdf = dat.drop(leftvar, axis=1).melt(id_vars= keyvar)
srdf['variable'].unique()
srdf['Year']= np.where(srdf['variable'].str.contains('2021'),2021,2022)
srdf['Provision']= srdf['variable'].str.replace('\d+', '')
#srdf= srdf.pivot_table(index= keyvar+ ['Provision'], columns='Year', values='value', aggfunc= 'min').reset_index()




#uniform the uper lower case
# investVar = ['InvestmentAreaLevel1a','InvestmentAreaLevel1b','InvestmentAreaLevel1c']
# for i in investVar:
#     invest[i]= invest[i].str.strip().str.capitalize()

#clean district in level of government for DC
# invest['Level_of_Goverment']= np.where(invest['Level_of_Goverment']=="District", "City",invest['Level_of_Goverment'])
# invest['Level_of_Goverment'].unique()
# # melt investment area
# invest.columns
# investvar =[x for x in invest.columns if x[:19] != "InvestmentAreaLevel"]
# investL = invest.melt(id_vars= investvar, var_name="InvestmentAreaVariable", value_name="InvestmentArea")


# #map investment area to EC
# investMeta = dataobj.read_data(group="RFAInvestmentAreaMeta")
# investMeta['ECgrp']= 'EC'+investMeta['ECCategory'].str.slice(0,1)
# investMeta['InvestmentArea']= np.where(investMeta['InvestmentArea']=='Food insecurity (including SNAP Benefits)',
# 'Food insecurity (including snap benefits)',investMeta['InvestmentArea'])


       
# helper.mergechk(investL, investMeta, mergebycol=['InvestmentArea'], checkcol='Keywords')
# investL= investL.join(investMeta.set_index('InvestmentArea').loc[:,'ECgrp'], on='InvestmentArea')
# investL['ECgrp']= investL['ECgrp'].fillna('NoEC')

# #jurisidctions with confirmed 3 provisions
# prov3 = invest[keyvar + ['EvidenceBased','ImpactEvaluation','DataEvidence']].\
#     melt(id_vars= keyvar).query("value=='Yes'").drop_duplicates()



#EC
ECdat = dataobj.read_data(group="EC")
#sort data
ECdat = helper.sort_jurisidiction(ECdat)
helper.check_STAbbr(ECdat)

keyvar = [x for x in ECdat.columns if x[:2] != "EC" ]
ECdat = ECdat.melt(id_vars=keyvar)
ECdat['variable'].unique()
#conver to numbers
numVar = ['Expenditure', 'ExpenditureSinceLastReport', 'Expenditure_Interim', 'Expenditure_2011','value']
for i in numVar:
    ECdat[i]= pd.to_numeric(ECdat[i], errors='coerce')

ECdat['Year']= np.where(ECdat['variable'].str.contains('2021'),'2021',\
    np.where(ECdat['variable'].str.contains('2022'),'2022', 'Interim'))
ECdat['ECgrp']= ECdat['variable'].str.split('_').str[0]
# ECdat=ECdat[ECdat['value'] >=0]

ECdat1= ECdat.pivot_table(index= ['Level_of_Goverment','Jurisdiction','ECgrp'], columns='Year', values='value', aggfunc= 'max').reset_index()
ECdatOthers = ECdat[keyvar].drop_duplicates()
ECdat1 = ECdat1.join(ECdatOthers.set_index(['Level_of_Goverment','Jurisdiction']), on=['Level_of_Goverment','Jurisdiction'])
# keyvar
len(ECdat1['Jurisdiction'].unique())
#check any juridication in EC but not in dat
ECid = ECdat['Jurisdication'].unique()
datid = dat['Jurisdication'].unique()
set(datid)-set(ECid)==set()
(set(ECid)-set(datid))==set()

#linkEC and overall award data for the desktop view
datL.columns
ECdat.columns
v1= ['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication',
       'Jurisdiction','variable', 'value']
v2= ['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication',
       'Jurisdiction','variable','value','Expenditure']
tst = pd.concat( [datL[v1], ECdat[v2]])
tst['variable'].unique()
tst = tst.join(dat.set_index(['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication',
       'Jurisdiction']).loc[:,['Population','SLFRF_Award','Expended_Funds','URL_I']], on=['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication',
       'Jurisdiction'])

#output data for tableau
helper.output_to_excel(config['output']['path'], srdf,'ARPResponse.xlsx' )
helper.output_to_excel(config['output']['path'], investL,'ARPInvestmentActivity.xlsx' )
helper.output_to_excel(config['output']['path'], ECdat1,'ARPEC.xlsx' )
helper.output_to_excel(config['output']['path'], tst,'ARPDataEC.xlsx' )
helper.output_to_excel(config['output']['path'], tst,'ARPDataEC.xlsx' )

# investMeta.to_excel("N:/Project/51448_ARPA/DC1/6. Data/mockup/output/InvestmentAreaMeta.xlsx", index=False)
#test to pull link
# from openpyxl import load_workbook

pinvestL=pinvestL.sort_values(['STAbbr','Jurisdiction'])
helper.output_to_excel(config['output']['path'], pinvestL,'PlanInvest.xlsx' )
helper.output_to_excel(config['output']['path'], rankL,'Ranking.xlsx' )
helper.output_to_excel(config['output']['path'], invest,'InvestmentProject.xlsx' )