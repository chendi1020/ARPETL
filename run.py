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

configinput['data21']

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




#add EC data
ECr = dataobj.ECdat()
ECr = helper.sort_jurisidiction(ECr)


#old ranking
oldrank = dataobj.readold()
oldrank.columns
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


eproject = eproj[  (eproj['StrategyName'].notnull()) |  (eproj['ConfirmEvaluation']==1) | (eproj['ConfirmDataEvidence']==1)]
#eproject =eproj[eproj['StrategyName'].notnull()]
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

eproject = helper.sort_jurisidiction(eproject)
helper.check_STAbbr(eproject)

#convert to numeric
numlist = ['ActivityFund','EvidenceBasedAmount','ImpactEvaluationAmount','DataEvidenceAmount']
for i in numlist:
    eproject[i]= pd.to_numeric(eproject[i], errors='coerce')

var= ['State', 'Jurisdication','Level_of_Goverment','SLFRF_Award',
       'Population','URL_I']
eproject= eproject.join(dat[var].set_index(['State', 'Jurisdication','Level_of_Goverment']), on=['State', 'Jurisdication','Level_of_Goverment'])

#eproject['InvestAreaMap1'].unique()


#output
helper.output_to_excel(config['output']['path'], pinvestL,'PlanInvest.xlsx' )
helper.output_to_excel(config['output']['path'], ECr,'TreasuryEC.xlsx' )
helper.output_to_excel(config['output']['path'], rankL,'Ranking.xlsx' )
helper.output_to_excel(config['output']['path'], eproject,'InvestmentProject.xlsx' )

