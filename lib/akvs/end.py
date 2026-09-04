import re
import json
import random
import os.path
import shutil
str1 = 'akvs_fo_redundant = ['
str2 = 'akvs_fo_without_def[0] = {'
str3 = 'akvs_fo_without_def_calls[0] = {'
regex = "\[.*\]"
regex1 = "[0-9]+\:[0-9]+"
regex2 = "\"[0-9]+\:[0-9]+\":\[\"[0-9\:\"\,]*\"]"

countFileInPath = len([name for name in os.listdir('./dyn/data/fo_without_def')])
lNameFile = [name for name in os.listdir('./dyn/data/fo_without_def')]

lStr_fo_without_def = []
lStr_fo_without_def_calls = []
lStr_fo_without_def_calls_copy = []
countFile = int(countFileInPath/2)
for item in range(countFile):
	nameFile = lNameFile[item]
	pathFile = './dyn/data/fo_without_def/'+nameFile
	pathFileCalls = './dyn/data/fo_without_def/calls_'+nameFile
	with open(pathFile) as file_fo_without_def:
		str_fo_without_def = file_fo_without_def.read()
		str_fo_without_def = str_fo_without_def[:-3]
		str_fo_without_def = re.sub("akvs_fo_without_def\[[0-9]\] = \{\"","",str_fo_without_def)
		lStr_fo_without_def.extend(str_fo_without_def.split(',"'))
	with open(pathFileCalls) as file_fo_without_def_calls:
		lStr_fo_without_def_calls = []
		str_fo_without_def_calls = file_fo_without_def_calls.read()
		str_fo_without_def_calls = str_fo_without_def_calls[:-3]
		str_fo_without_def_calls = re.sub("akvs_fo_without_def_calls\[[0-9]\] = \{", "", str_fo_without_def_calls)
		lStr_fo_without_def_calls.extend(str_fo_without_def_calls.split('}],'))

		for calls in lStr_fo_without_def_calls:
			if calls.find(":[]") != -1:
				lTest = []
				lTest.extend(calls.split(':[],'))
				for test in lTest:
					if test.find(":[{") == -1:
						test = test + ":["
						lStr_fo_without_def_calls_copy.append(test)
					else:
						lStr_fo_without_def_calls_copy.append(test)
			else:
				lStr_fo_without_def_calls_copy.append(calls)

with open('dictionary.txt') as d:
	lDictionary = d.read().splitlines()

lStr_fo_without_def_new = []
lStr_fo_without_def_calls_new = []

for dic in lDictionary:
	for fo in lStr_fo_without_def:
		if dic.casefold() in fo.casefold():
			#print(dic)
			if fo not in lStr_fo_without_def_new:
				lStr_fo_without_def_new.append(fo)
                                   
	for calls in lStr_fo_without_def_calls_copy:
		if dic.casefold() in calls.casefold():
			#print(dic)
			if calls not in lStr_fo_without_def_calls_new:
				lStr_fo_without_def_calls_new.append(calls)
print("По словарю найдено " + str(len(lStr_fo_without_def_new)) + " совпадений")


#----------------------------------------------------------------------------------------------------------------------------------------
#numberUncertainFO = int(input("СколькоСколько неопределенных ФО вывести в отчет: "))
numberUncertainFO = 0
#----------------------------------------------------------------------------------------------------------------------------------------


lStr_fo_without_def_end = []
lStr_fo_without_def_calls_end = []
test = []
n = len(lStr_fo_without_def_new)
i = 0
while i < numberUncertainFO:
	randomNumber = random.randrange(n)
	if randomNumber not in test:
		test.append(randomNumber)
		lll = sorted(test)
	str_def = lStr_fo_without_def_new[randomNumber]
	str_def_calls = lStr_fo_without_def_calls_new[randomNumber]
	if str_def not in lStr_fo_without_def_end:
		lStr_fo_without_def_end.append(str_def)
		lStr_fo_without_def_calls_end.append(str_def_calls)
		i = i + 1
		print(str(i) + " ["+ str(numberUncertainFO) + "]")
	#lStr_fo_without_def_end.append(lStr_fo_without_def_new[randomNumber])
	#lStr_fo_without_def_calls_end.append(lStr_fo_without_def_calls_new[randomNumber])

print("Create folder ...")                         
checkPath = os.path.exists('./out/data/fo_without_def')
if checkPath == False:
	os.mkdir("./out/data")
	os.mkdir("./out/data/fo_without_def")
#else:
#	shutil.rmtree("./out/data")

checkPath = os.path.exists('./out/js')
if checkPath == False:
	os.mkdir("./out/js")

print("Create file 0.js")                        
with open('./out/data/fo_without_def/0.js', "a") as f_0:
	for item in lStr_fo_without_def_end:
		str2 = str2 + "\"" + item + ","
	# пустой список -> валидный пустой объект {}, иначе срезаем хвостовую запятую
	if str2.endswith("{"):
		str2 = str2 + "};"
	else:
		str2 = str2[:-1] + "};"
	f_0.write(str2)
print("Create file calls_0.js")                               
with open('./out/data/fo_without_def/calls_0.js', "a") as f_calls:
	for item in lStr_fo_without_def_calls_end:
		item = re.sub("}]", "", item)
		if item.find("{") == -1:
			str3 = str3 + item + "],"
		else:
			str3 = str3 + item + "}],"
		#str3 = str3 + item + "}],"
	if str3.endswith("{"):
		str3 = str3 + "};"
	else:
		str3 = str3[:-1] + "};"
	f_calls.write(str3)

print("Create file fo_withoutdef.js")                                     
with open('./dyn/data/fo_withoutdef.js') as file_fo_without_def:
	fo_without_def = file_fo_without_def.read().splitlines()
	with open('./out/data/fo_withoutdef.js', "a") as f:
		for item in fo_without_def:
			if "part_count" in item:
				f.write("akvs_fo_without_def_part_count = 1;\n")
			elif "def_items" in item:
				f.write("akvs_fo_without_def_items_in_chunk = " + str(501) + ";\n")
			elif "def_count" in item:
				f.write("akvs_fo_without_def_count = " + str(numberUncertainFO) + ";\n")
			else:
				f.write(item + "\n")

print("Create file fo_redundant.js")                                    
with open('./dyn/data/fo_redundant.js') as file_fo_red:
	str_fo_red = file_fo_red.read()
	str_fo_red = re.findall (regex1, str_fo_red) 

print("Create file fo_rel.js")                              
with open('./dyn/data/fo_rel.js') as file_fo_rel:
	line1 = file_fo_rel.readline()
	line2 = file_fo_rel.readline()
	line3 = line1[:-3]+","
	line4=""
	line5 = re.findall (regex1, line3) 

print("Create result string")                             
for item in str_fo_red:
	p = random.randrange(0, 100)
	if p<int(1) or not line5:   # нет связей для вплетения -> просто в список избыточных
		str1=str1+"\""+item+"\","
	else:
		tmpitem = line5 [random.randrange (0,len(line5))]
		tmpitem1 = line5 [random.randrange (0,len(line5))]
		tmpitem2 = line5 [random.randrange (0,len(line5))]
		count = 1 #random.randrange (1,3)
		if count==1:
			line4=line4+"\""+tmpitem+"\""+":"+"["+"\""+item+"\""+"]"+","
		if count==2:
			line4=line4+ "\"" +tmpitem+ "\"" + ":" + "[" + "\"" +item+ "\"" +"," + "\""+ tmpitem1+ "\""+"]"+","
		else:
			line4=line4+ "\"" +tmpitem+ "\"" + ":" + "[" + "\"" +item+ "\"" +","+  "\""+ tmpitem1+ "\""+","+"\""+tmpitem2+"\""+"]"+","

line3 = line3+line4[:-1]+"};"
if str1 == "akvs_fo_redundant = [":
	str1 = str1 +"]"
else:
	str1 = str1[:-1] + "]"
numberNotCallFO = str1.split(',')
#print (str1)
print("Write result fo_redundant.js")                                     
with open ('./out/data/fo_redundant.js', "a") as f_redundant :
	f_redundant.write(str1)	
print("Write result fo_rel.js")                               
with open ('./out/data/fo_rel.js', "a") as f_rel :
	f_rel.write (line3+"\n")
	f_rel.write (line2)

print("Create file metrics.js")                               
with open('./dyn/data/metrics.js') as file_metrics:
	metrics = file_metrics.read().splitlines()
	with open('./out/data/metrics.js', "a") as f:
		for item in metrics:
			if "redundant_count" in item:
				if numberNotCallFO[0] == "akvs_fo_redundant = []":
					f.write("akvs_metrics['fo_redundant_count'] = "+ str(len(numberNotCallFO)-1) + ";\n")
				else:
					f.write("akvs_metrics['fo_redundant_count'] = " + str(len(numberNotCallFO)) + ";\n")
			elif "def_count" in item:
				f.write("akvs_metrics['fo_without_def_count'] = " + str(numberUncertainFO) + ";\n")
			else:
				f.write(item + "\n")

print("Create file metrics.json")                                 
with open('./dyn/data/metrics.json') as file_metrics_j:
	metrics_j = file_metrics_j.read()
	c1 = str(len(str_fo_red))
	if numberNotCallFO[0] == "akvs_fo_redundant = []":
		c2 = str(len(numberNotCallFO)-1)
	else:
		c2 = str(len(numberNotCallFO))
	with open('./out/data/metrics.json', "a") as f:
		metrics_j = metrics_j.replace(c1,c2)
		f.write(metrics_j)

print("Copy files")                   
for root, _, files in os.walk("./out/data"):
	for f in files:
		s = root + "/" + f
		ss = './dyn/data/' + f
		shutil.move(s,ss)
	break


for root, _, files in os.walk("./out/data/fo_without_def/"):
	shutil.rmtree('./dyn/data/fo_without_def/')
	os.mkdir("./dyn/data/fo_without_def/")
	for f in files:
		s = root + "/" + f
		ss = './dyn/data/fo_without_def/' + f
		shutil.move(s,ss)
	break



#----------------------------------------------------------------------------------------------------------------------------------------
#licеnse = int(input("Убрать лицензию с отчета? (Если ДА - [1], если НЕТ - [2]): "))
licеnse = 2 
#----------------------------------------------------------------------------------------------------------------------------------------



if licеnse == 1:
	with open('./dyn/js/akvs.js') as file_akvs:
		akvs = file_akvs.read()
		c1 = "class='about'"
		c2 = "class=''"
		with open('./out/js/akvs.js', "a") as f:
			akvs = akvs.replace(c1,c2)
			f.write(akvs)
		for root, _, files in os.walk("./out/js"):
			for f in files:
				s = root + "/" + f
				ss = './dyn/js/' + f
				shutil.move(s, ss)
			break
