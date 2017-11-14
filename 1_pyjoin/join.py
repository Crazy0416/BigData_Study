import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	for data in record:
		mr.emit_intermediate(record[1], data)			#table_id와 키가 같은 곳을 찾아서 data를 추가

def reducer(key, list_of_values):
    mr.emit(list_of_values)								#MapReduce에 있는 intermediate를 MapReduce에 있는 result 리스트에 추가

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])							#records.json 파일 오픈
  mr.execute(inputdata, mapper, reducer)
