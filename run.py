import importlib
from pathlib import Path
import os
import pandas as pd
import helper
import numpy as np
import re
import TreasuryEC


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
#funding and rating
#dat is fund
dat = dataobj.read_data(group="Fund")
numlist = ['SLFRF_Award','Population', 'Award_Per_Capita', 'Num_Projects']+[x for x in dat.columns if "Score" in x]
for i in numlist:
    dat[i]= pd.to_numeric(dat[i], errors='coerce')
dat['Score_2022']= dat[[x for x in dat.columns if "Score" in x]].sum(axis=1)
dat = helper.sort_jurisidiction(dat)
dat.columns

#sort data
logger.info("Fund dataframe "+ helper.check_STAbbr(dat)) 

#add plan url to plan
var= ['State', 'Jurisdication','Level_of_Goverment','SLFRF_Award',
       'Population','URL_I']
pinvestL= pinvestL.join(dat[var].set_index(['State', 'Jurisdication','Level_of_Goverment']), on=['State', 'Jurisdication','Level_of_Goverment'])
pinvestL=pinvestL.sort_values(['STAbbr','Jurisdiction'])
#add rowid by group for select unique
pinvestL['rid']=1
pinvestL['rid']= pinvestL.groupby(['State', 'Jurisdication','Level_of_Goverment'])['rid'].cumsum()
# add total number of investment by jurisdiction
pinvestL['valuenum']= np.where(pinvestL['value']=='Y',1,0)
pinvestL['TotalInvestNum']= pinvestL.groupby(['State', 'Jurisdication','Level_of_Goverment'])['valuenum'].transform(sum)
helper.output_to_excel(config['output']['path'], pinvestL,'PlanInvest.xlsx' )




#add EC data
ECr = TreasuryEC.ECdat()
ECr.columns
# ECr= ECr.drop("State", axis=1).rename(columns={"State or territory":"State","Level of government":"Level_of_Goverment",
# "Jurisdiction":"Jurisdication"})
ECr = helper.sort_jurisidiction(ECr)
# varkeep =['Jurisdiction','STAbbr','Level_of_Goverment','Population']
# ECr= ECr.join(dat[varkeep].set_index(['Jurisdiction','STAbbr','Level_of_Goverment']), on=['Jurisdiction','STAbbr','Level_of_Goverment'])
# t=ECr.query("Jurisdiction != Jurisdiction & variable=='SLFRF_Award'")
helper.output_to_excel(config['output']['path'], ECr[ECr["Year"]==2022],'TreasuryEC.xlsx' )

#old ranking
oldconfig = config['data21']
olddata = ip.InputData(**oldconfig)
oldrank = olddata.readold()
oldrank.columns
# oldrank['Jurisdiction']= np.where( (oldrank['Level_of_Goverment']=='State') & (oldrank['Jurisdication'] !='Navajo Nation')  & 
# (oldrank['Jurisdication'] !='Cherokee Nation'), oldrank['STAbbr'],
#         np.where(oldrank['STAbbr']=="DC", "District of Columbia",  oldrank['Jurisdication']+","+oldrank['STAbbr'] )
#       )
#t = oldrank.groupby(['Jurisdiction','State']).size()

rank = dat.join(oldrank.set_index(['Jurisdiction','STAbbr','Level_of_Goverment']).drop(['Jurisdication','State'], axis=1),
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
#add 2022 score
score = rankL1.query("Group=='Score'").rename(columns={"2022":"Composite2022","2021":"Composite2021"}).drop(["Group","Change"], axis=1)
rankL = rankL.join(score.set_index("Jurisdiction"), on="Jurisdiction")


##investment area and coding
investmapping=config['input']['investAreaMapping']
def mapinvest(i):
    for k in investmapping.keys():
        for j in investmapping[k]:
            if i==j:
                return k
    return None 
eproj = dataobj.read_data(group="InvestmentArea")
eproj.columns
eproj['InvestAreaMap1']= eproj['InvestmentAreaLevel1'].transform(mapinvest)
eproj['InvestAreaMap2']= eproj['InvestmentAreaLevel2'].transform(mapinvest)

eproject =eproj[eproj['StrategyName'].notnull()]
unmatched = ((eproject['InvestmentAreaLevel2'].notnull()) & (eproject['InvestAreaMap2'].isnull()) ) | ( (
 eproject['InvestmentAreaLevel1'].notnull()) & (eproject['InvestAreaMap1'].isnull()))

#those could not being mapped
t= eproject[unmatched ]
#recode to Misc
# eproject['InvestAreaMap1']= np.where(unmatched & (eproject['InvestAreaMap1'].isnull()) & (eproject['InvestAreaMap2'].isnull()), 
# 'Misc',eproject['InvestAreaMap1'])
#fill missing with original
eproject['InvestAreaMap1']= np.where(eproject['InvestAreaMap1'].isnull(), eproject['InvestmentAreaLevel1'],eproject['InvestAreaMap1'])
eproject['InvestAreaMap2']= np.where(eproject['InvestAreaMap2'].isnull(), eproject['InvestmentAreaLevel2'],eproject['InvestAreaMap2'])
#for now include all
# will need to futher filter 
# eproject= eproj[(eproj['Activity'].notnull()) &  (eproj['Jurisdication'].notnull()) & (eproj['Level_of_Goverment'].notnull())
# & (eproj['ImpactEvaluation'].notnull()) & (eproj['DataEvidence'].notnull()) & (eproj['EvidenceBased'].notnull()) & (eproj['Agency'].notnull()) ]
#match with the investment area

eproject = helper.sort_jurisidiction(eproject)
helper.check_STAbbr(eproject)

#convert to numeric
numlist = ['ActivityFund','EvidenceBasedAmount','ImpactEvaluationAmount','DataEvidenceAmount']
for i in numlist:
    eproject[i]= pd.to_numeric(eproject[i], errors='coerce')

var= ['State', 'Jurisdication','Level_of_Goverment','SLFRF_Award',
       'Population','URL_I']
eproject= eproject.join(dat[var].set_index(['State', 'Jurisdication','Level_of_Goverment']), on=['State', 'Jurisdication','Level_of_Goverment'])

eproject['InvestAreaMap1'].unique()

# sr = config['input']['sr']
# #drop sr transpose available fund
# dat['AvailableFund']= dat['SLFRF_Award']- dat['Expended_Funds'].fillna(0) #fill missing for now. might need to remove with prod data
# keyvar = ['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication','Jurisdiction']
# datL = dat.drop(sr+['RecoveryPlan_2021', 'RecoveryPlan_Interim', 'RecoveryPlan_2022',
# 'URL_G', 'URL_H', 'URL_I','Num_Projects','Expenditure_EC34'], axis=1).melt(id_vars= keyvar+['Population'])
# datL['variable'].unique()
# datL=datL.join(dat.set_index(keyvar+['Population']), on =keyvar+['Population'])


# #strength of response melt
# leftvar = [x for x in dat.columns if x not in sr and x not in keyvar ]
# srdf = dat.drop(leftvar, axis=1).melt(id_vars= keyvar)
# srdf['variable'].unique()
# srdf['Year']= np.where(srdf['variable'].str.contains('2021'),2021,2022)
# srdf['Provision']= srdf['variable'].str.replace('\d+', '')
# #srdf= srdf.pivot_table(index= keyvar+ ['Provision'], columns='Year', values='value', aggfunc= 'min').reset_index()




# #uniform the uper lower case
# # investVar = ['InvestmentAreaLevel1a','InvestmentAreaLevel1b','InvestmentAreaLevel1c']
# # for i in investVar:
# #     invest[i]= invest[i].str.strip().str.capitalize()

# #clean district in level of government for DC
# # invest['Level_of_Goverment']= np.where(invest['Level_of_Goverment']=="District", "City",invest['Level_of_Goverment'])
# # invest['Level_of_Goverment'].unique()
# # # melt investment area
# # invest.columns
# # investvar =[x for x in invest.columns if x[:19] != "InvestmentAreaLevel"]
# # investL = invest.melt(id_vars= investvar, var_name="InvestmentAreaVariable", value_name="InvestmentArea")


# # #map investment area to EC
# # investMeta = dataobj.read_data(group="RFAInvestmentAreaMeta")
# # investMeta['ECgrp']= 'EC'+investMeta['ECCategory'].str.slice(0,1)
# # investMeta['InvestmentArea']= np.where(investMeta['InvestmentArea']=='Food insecurity (including SNAP Benefits)',
# # 'Food insecurity (including snap benefits)',investMeta['InvestmentArea'])


       
# # helper.mergechk(investL, investMeta, mergebycol=['InvestmentArea'], checkcol='Keywords')
# # investL= investL.join(investMeta.set_index('InvestmentArea').loc[:,'ECgrp'], on='InvestmentArea')
# # investL['ECgrp']= investL['ECgrp'].fillna('NoEC')

# # #jurisidctions with confirmed 3 provisions
# # prov3 = invest[keyvar + ['EvidenceBased','ImpactEvaluation','DataEvidence']].\
# #     melt(id_vars= keyvar).query("value=='Yes'").drop_duplicates()



# #EC
# ECdat = dataobj.read_data(group="EC")
# #sort data
# ECdat = helper.sort_jurisidiction(ECdat)
# helper.check_STAbbr(ECdat)

# keyvar = [x for x in ECdat.columns if x[:2] != "EC" ]
# ECdat = ECdat.melt(id_vars=keyvar)
# ECdat['variable'].unique()
# #conver to numbers
# numVar = ['Expenditure', 'ExpenditureSinceLastReport', 'Expenditure_Interim', 'Expenditure_2011','value']
# for i in numVar:
#     ECdat[i]= pd.to_numeric(ECdat[i], errors='coerce')

# ECdat['Year']= np.where(ECdat['variable'].str.contains('2021'),'2021',\
#     np.where(ECdat['variable'].str.contains('2022'),'2022', 'Interim'))
# ECdat['ECgrp']= ECdat['variable'].str.split('_').str[0]
# # ECdat=ECdat[ECdat['value'] >=0]

# ECdat1= ECdat.pivot_table(index= ['Level_of_Goverment','Jurisdiction','ECgrp'], columns='Year', values='value', aggfunc= 'max').reset_index()
# ECdatOthers = ECdat[keyvar].drop_duplicates()
# ECdat1 = ECdat1.join(ECdatOthers.set_index(['Level_of_Goverment','Jurisdiction']), on=['Level_of_Goverment','Jurisdiction'])
# # keyvar
# len(ECdat1['Jurisdiction'].unique())
# #check any juridication in EC but not in dat
# ECid = ECdat['Jurisdication'].unique()
# datid = dat['Jurisdication'].unique()
# set(datid)-set(ECid)==set()
# (set(ECid)-set(datid))==set()

# #linkEC and overall award data for the desktop view
# datL.columns
# ECdat.columns
# v1= ['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication',
#        'Jurisdiction','variable', 'value']
# v2= ['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication',
#        'Jurisdiction','variable','value','Expenditure']
# tst = pd.concat( [datL[v1], ECdat[v2]])
# tst['variable'].unique()
# tst = tst.join(dat.set_index(['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication',
#        'Jurisdiction']).loc[:,['Population','SLFRF_Award','Expended_Funds','URL_I']], on=['Level_of_Goverment', 'State', 'STAbbr', 'Jurisdication',
#        'Jurisdiction'])

# #output data for tableau
# helper.output_to_excel(config['output']['path'], srdf,'ARPResponse.xlsx' )
# helper.output_to_excel(config['output']['path'], investL,'ARPInvestmentActivity.xlsx' )
# helper.output_to_excel(config['output']['path'], ECdat1,'ARPEC.xlsx' )
# helper.output_to_excel(config['output']['path'], tst,'ARPDataEC.xlsx' )
# helper.output_to_excel(config['output']['path'], tst,'ARPDataEC.xlsx' )

# # investMeta.to_excel("N:/Project/51448_ARPA/DC1/6. Data/mockup/output/InvestmentAreaMeta.xlsx", index=False)
# #test to pull link
# # from openpyxl import load_workbook


helper.output_to_excel(config['output']['path'], rankL,'Ranking.xlsx' )
helper.output_to_excel(config['output']['path'], eproject,'InvestmentProject.xlsx' )

