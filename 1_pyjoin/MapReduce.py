# Copyright 2013 University of Washington eScience Institute
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import json

class MapReduce:
    def __init__(self):         #인스턴스 초기화
        self.intermediate = {}      #딕셔너리 변수 선언
        self.result = []            #리스트 변수 선언 

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])       #딕셔너리에 키값이 없다면 리스트 변수 생성 있으면 출력 
        self.intermediate[key].append(value)        #딕셔너리에 존재하는 키값에 column들을 삽입 

    def emit(self, value):
        self.result.append(value)                   #result 리스트에 value 삽입 

    def execute(self, data, mapper, reducer):
        for line in data:
            record = json.loads(line)               #records.json 데이터를 한 줄 씩 받아온다. 
            mapper(record)                          #mapper 메소드 실행 

        for key in self.intermediate:               
            reducer(key, self.intermediate[key])    #MapReduce에 있는 intermediate를 MapReduce에 있는 result 리스트에 추가

        #jenc = json.JSONEncoder(encoding='latin-1')
        jenc = json.JSONEncoder()
        for item in self.result:
            print jenc.encode(item)                 # item 출력 
