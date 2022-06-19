import os
queryPath = "/home/mist/stack-xmlprocess/queries.txt"
originPath = "/home/mist/stack-xmlprocess/origin_query.txt"
with open(queryPath,"r") as f1:
    with open(originPath,"w") as f2:
        for line in f1.readlines():
            f2.write(f'{line.split(" ===> ")[1]}\n')