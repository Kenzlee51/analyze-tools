import re
import random
import shutil
import os

regex2 = "\"[0-9]+\:[0-9]+\":\[\"[0-9\:\"\,]*\"]"
regex3 = "\"[0-9]+\:[0-9]+\":"
regex4 = "[0-9]+\:[0-9]+"

max_prob_fun = 95 #95
max_prob_branch  = 93 #93




checkPath = os.path.exists('./out/')
if checkPath == False:
	os.mkdir("./out/")
else:
    shutil.rmtree('./out/')
    os.mkdir("./out/")


with open('./in/data/fo_rel.js') as file_fo_rel:
	str_fo_rel = file_fo_rel.read()
	str_fo_rel = re.findall (regex2, str_fo_rel) 
for item in str_fo_rel:
	if (random.randrange (0,100)<max_prob_fun):
		i=0
		list_branch = re.findall (regex4, item)
		for branch in list_branch:
			with open('./out/trace.log','a') as fTrace:		
				if (i==0):
					tmp=branch
					fTrace.write (branch+":f:i\n")			
					i=i+1
				else:
					fTrace.write (branch+":f:i\n")
					fTrace.write (branch+":f:o\n")
					i=i+1
				if (i==len(list_branch)):
					fTrace.write (tmp+":f:o\n")				
									

with open('./in/data/fo.js') as file_fo:
	str_fo = file_fo.read()
	str_fo= re.findall (regex4, str_fo)
	print (len(str_fo)) 	
	for fo in str_fo:
		with open('./out/trace.log','a') as fTrace:		
			fTrace.write (fo+":f:i\n")			
			fTrace.write (fo+":f:o\n")	


with open('./in/data/fo_branch_rel.js') as file_fo_branch_rel:
    str_fo_branch_rel = file_fo_branch_rel.read()
    str_fo_branch_rel = re.findall (regex2, str_fo_branch_rel) 

for item in str_fo_branch_rel:
    if (random.randrange (0,100)<max_prob_branch):
        i=0
        list_branch = re.findall (regex4, item)
        for branch in list_branch:
            with open('./out/trace.log','a') as fTrace:		
                if (i==0):
                    tmp=branch
                    fTrace.write (branch+":f:i\n")			
                    i=i+1
                else:
                    fTrace.write (branch+":b:i\n")
                    fTrace.write (branch+":b:o\n")
                    i=i+1
                if (i==len(list_branch)):
                    fTrace.write (tmp+":f:o\n")				
