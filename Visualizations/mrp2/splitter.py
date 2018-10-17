#!/usr/bin/python

import sys  
import os

words = open('words','w')
count = open('count','w')

filepath = '../../results/mrp2'  
with open(filepath) as fp:  
  	for cnt, line in enumerate(fp):
		w,n=line.rstrip().split("\t")
		
		if(int(n)>750):
			words.write("\""+w+"\", ")
			count.write("\""+w+"\":"+n+", ")

count.close()
words.close()
fp.close()
