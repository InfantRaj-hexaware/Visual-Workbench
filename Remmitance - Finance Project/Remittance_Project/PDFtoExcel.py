import regex as re
import os
import csv

# java_cmd = "java -jar E:\\CTO\\Vaas_Supporting_files\\pdfbox-app-2.0.22.jar ExtractText"
java_cmd = "java -jar D:\\CTO\\Remmitance\\Remittance_Project\\pdfbox-app-2.0.22.jar ExtractText"
filename = "1000069_276663.pdf"  
name = filename[:filename.rindex('.')]
extracted_textpath = name + ".txt"
os.system(java_cmd + " " + filename + " " + extracted_textpath)
print("os system path")
print(java_cmd + " " + filename + " " + extracted_textpath)
with open(extracted_textpath,'r') as f:
    text = f.read()
# import tika
# tika.initVM()
# from tika import parser
# parsed = parser.from_file(filename)
# text = parsed["content"].encode("utf-8")
name = filename[:filename.rindex('.')]
# print (text)
res = re.finditer(r'([0-9\,\.]+)\s([0-9\,\.]+)\s([0-9\,]+\.\d{2})([0-9\-]+)\s?([A-z\'+)?\s?([A-z]+)?\s([0-9\/]+)\sCC\n([0-9\.]+)Hrs\s([a-z]+)\s([a-z]+)\s\$([0-9\.]+)\s\-\s(([A-z\'])+\s?([A-z]+))',text)
# print(res[0][0], res[0][1], res[0][2], res[0][3],res[0][5],res[0][10])
Hours = []
Name = []
Revenue = []
InvoiceNo = []
InvoiceDate = []
Discount = []
NetAmount = []


for i in res:
    print(i.group(1),i.group(2),i.group(3),i.group(4),i.group(5),i.group(6),i.group(7),i.group(8),i.group(9),i.group(10),i.group(11),i.group(12),i.group(13))
    Hours.append(i.group(7).strip())
    Name.append(i.group(11).strip())
    Revenue.append(i.group(1).strip())
    InvoiceNo.append(i.group(4).strip())
    InvoiceDate.append(i.group(6).strip())
    Discount.append(i.group(2).strip())
    NetAmount.append(i.group(3).strip())
# Invoice_no = []
# date = []
# Revenue = []
# Hours = []
# for i in res:

#         Name.append(i.group(4).strip())
#         date.append(i.group(10).strip())
#         Revenue.append(i.group(1).strip())
#         Hours.append(i.group(11).strip())
#         print(i.group(4),i.group(10),i.group(1),i.group(11))

to_dict={
            "Hours":Hours,
            "Name":Name,
            "Revenue":Revenue,
            "InvoiceNo":InvoiceNo,
            "InvoiceDate":InvoiceDate,
            "Discount":Discount,
            "NetAmount":NetAmount
        }
print("Excel size is = ",len(to_dict['Hours']))

import pandas as pd

d_frames=pd.DataFrame(to_dict)
        #df.drop('reports', axis=1)
d_frames.to_csv(name + '.csv',index = False)
