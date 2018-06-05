#!/opt/anaconda/anaconda2.7/bin/python
# -*- coding: iso-8859-9 -*-
import requests
import json
import time
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from datetime import datetime, timedelta

#setting the variables
today = datetime.utcnow()
gunun_tarihi = today.strftime("%Y.%m.%d")
indexName = "netflow-"
okunan_dokuman_sayisi=0
okunacak_dokuman_sayisi=10000
dokuman_sayisi=0
last_sort =[]
search_after_state="False"

def process_hits(hits):
	global last_sort
	log_yaz(datetime.utcnow(),"process_hits fonksiyonu calisti","","")
	for item in hits:
		json_str = json.dumps(item,separators=(',', ': '))
		data1 = json.loads(json_str) 
		last_sort = data1["sort"]
    #print data1["_source"]["netflow"]["ipv4_src_addr"],data1["_source"]["netflow"]["l4_src_port"],data1["_source"]["netflow"]["ipv4_dst_addr"],data1["_source"]["netflow"]["l4_dst_port"]
    # You can use your own process function in here

#check date whether is change or not
def tarih_kontrol(tarih):
	global okunan_dokuman_sayisi
	global search_after_state
	today = datetime.now()
	log_yaz(datetime.utcnow(),"tarih kontrol fonksiyonu calisti: tarih: datetime.now:",tarih,today.strftime("%Y.%m.%d"))
	if tarih != today.strftime("%Y.%m.%d"):
		okunan_dokuman_sayisi = 0
		search_after_state="False"
		index_tarihini_bugun_yap()
    
#change index date    
def index_tarihini_bugun_yap():
	today = datetime.now()
	global gunun_tarihi
	gunun_tarihi = today.strftime("%Y.%m.%d")
	log_yaz(datetime.utcnow(),"gunun tarihi degisiyor index_tarihini_bugun_yap fonksiyonu calisti",gunun_tarihi," ")
  
#get document count  
def dokuman_sayisi_getir():
	hits_total=es.search(index=indexName+gunun_tarihi, filter_path=['hits.total'])
	json_str = json.dumps(hits_total,separators=(',', ': '))
	json1 = json.loads(json_str) 
	log_yaz(datetime.utcnow(),"dokuman sayisi aliniyor...:",json1["hits"]["total"],"")
	return (json1["hits"]["total"])
  
#write log  
def log_yaz(str1,str2,str3,str4):
	log_dosyasi = open('/var/log/search_after.log', 'a')
	log_dosyasi.write(("%s: %s: %s: %s\n") % (str1, str2, str3, str4))
	log_dosyasi.close()
  
#this function is used for read first 1 document from index.   
def search_after_initialization():
	global search_after_state
	global okunan_dokuman_sayisi
	global okunacak_dokuman_sayisi
	if search_after_state == "False":
		okunacak_dokuman_sayisi=1
		log_yaz(datetime.utcnow(),"search_after_initialization fonksiyonu calisti",gunun_tarihi,str(okunan_dokuman_sayisi))
		if es_index.exists(index=indexName+gunun_tarihi):
			log_yaz(datetime.utcnow(),"index mevcut",indexName+gunun_tarihi,"")
			data = es.search(index=indexName+gunun_tarihi,size=okunacak_dokuman_sayisi,body={
			"query": {
			"match_all" : {}
			},
			#"search_after": [last_timestamp , last_id],
			"sort": [
			{"@timestamp": "asc"},
			{"_uid": "asc"}	
			]
			})
			process_hits(data['hits']['hits'])
			search_after_state="True"
			okunan_dokuman_sayisi=okunacak_dokuman_sayisi
			okunacak_dokuman_sayisi=10000
		else:
			log_yaz(datetime.utcnow(),"index mevcut degil",indexName+gunun_tarihi,"")
			time.sleep(120)
			search_after_initialization()
      
#read documents from index      
def dokuman_oku(okunacak_dokuman_sayisi,last_sort):
	global data
	data = es.search(index=indexName+gunun_tarihi,size=okunacak_dokuman_sayisi,body={
		
		"query": {
			"match_all" : {}
		},
		"search_after": last_sort,
		"sort": [
			{"@timestamp": "asc"},
			{"_uid": "asc"}
			
		]
		})
    
    
#connecting to elasticsearch
es = Elasticsearch([{'host': 'HOSTNAME OF ELASTICSEARCH', 'port': 9200}])
es_index = IndicesClient(es)	
index_tarihini_bugun_yap()    

while True:
	search_after_initialization()
	dokuman_sayisi = dokuman_sayisi_getir()
	if (dokuman_sayisi-okunan_dokuman_sayisi) > 9999:
		log_yaz(datetime.utcnow(),"10000 dokuman okunuyor. Toplam okunan_dokuman_sayisi: Okunacak Dokuman Sayisi:",str(okunan_dokuman_sayisi),str(okunacak_dokuman_sayisi))
		dokuman_oku(okunacak_dokuman_sayisi,last_sort)
		process_hits(data['hits']['hits'])
		okunan_dokuman_sayisi = okunan_dokuman_sayisi + okunacak_dokuman_sayisi
		log_yaz(datetime.utcnow(),"okunan dokuman sayisi artiriliyor: okunan dokuman sayisi , okunacak dokuman sayisi ",str(okunan_dokuman_sayisi),str(okunacak_dokuman_sayisi))
	elif (dokuman_sayisi-okunan_dokuman_sayisi) < 10000 and (dokuman_sayisi-okunan_dokuman_sayisi) > 0:
		okunacak_dokuman_sayisi = dokuman_sayisi - okunan_dokuman_sayisi
		log_yaz(datetime.utcnow(),"okunacak_dokuman_sayisi 10000 den az: okunacak_dokuman_sayisi: dokuman_sayisi",str(okunacak_dokuman_sayisi),str(dokuman_sayisi))
		dokuman_oku(okunacak_dokuman_sayisi,last_sort)
		process_hits(data['hits']['hits'])
		okunan_dokuman_sayisi = okunan_dokuman_sayisi + okunacak_dokuman_sayisi
		okunacak_dokuman_sayisi=10000
		log_yaz(datetime.utcnow(),"okunan dokuman sayisi artiriliyor: okunan dokuman sayisi , okunacak dokuman sayisi ",str(okunan_dokuman_sayisi),str(okunacak_dokuman_sayisi))
		time.sleep(10)
	else:
		time.sleep(30)
		log_yaz(datetime.utcnow(),"okunacak_dokuman_sayisi 0. Toplam okunan_dokuman_sayisi: Toplam Dokuman Sayisi:",str(okunan_dokuman_sayisi),str(dokuman_sayisi))
		tarih_kontrol(gunun_tarihi)
