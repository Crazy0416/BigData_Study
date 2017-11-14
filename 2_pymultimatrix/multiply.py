import MapReduce
import sys

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	if(record[0] == 'a'):
		for num in range(0, 5):
			interKey = str(record[1])+str(num)+str(record[2])
			mr.emit_intermediate(interKey, record[3])
	else:
		for num2 in range(0,5):
			interKey2 = str(num2)+str(record[2])+str(record[1])
			mr.emit_intermediate(interKey2, record[3])

def reducer(key, list_of_values):
	aVal = list_of_values[0];
	bVal = list_of_values[1] if len(list_of_values)==2 else 1
	red_val = [key[0], key[1], aVal*bVal]
	mr.emit(red_val)
	# aVal = list_of_values[2];
	# bVal = list_of_values[5] if len(list_of_values)==6 else 1
	
	# print "value : "
	# print aVal
	# print bVal

	# value = [list_of_values[0], list_of_values[1], aVal*bVal];
	# print "Result : "
	# print value

	# mr.emit(value)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
