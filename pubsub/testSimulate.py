from send_meter_data import splitRow

# print(splitRow('2017-03-31T20:00:00-04:00,6443.0,1941.0,40.0,5397.0,2590.0', 
#                 'timestamp,1_Gen,1_Sub_1,1_Sub_3,2_Gen,2_Sub_1'))
assert(splitRow('2017-03-31T20:00:00-04:00,6443.0,1941.0,40.0,5397.0,2590.0', 
                'timestamp,1_Gen,1_Sub_1,1_Sub_3,2_Gen,2_Sub_1') == 
                ['2017-03-31T20:00:00-04:00,1,6443.0,1941.0,40.0', 
                  '2017-03-31T20:00:00-04:00,2,5397.0,2590.0'])
print("Test1 Passed!")
# print(splitRow('2017-03-31T20:00:00-04:00,6443.0,1941.0,40.0,5397.0,2590.0,0.0', 
#                 'timestamp,1_Gen,1_Sub_1,1_Sub_3,2_Gen,2_Sub_1,3_Gen'))
assert(splitRow('2017-03-31T20:00:00-04:00,6443.0,1941.0,40.0,5397.0,2590.0,0.0', 
                'timestamp,1_Gen,1_Sub_1,1_Sub_3,2_Gen,2_Sub_1,3_Gen') == 
                ['2017-03-31T20:00:00-04:00,1,6443.0,1941.0,40.0', 
                  '2017-03-31T20:00:00-04:00,2,5397.0,2590.0', 
                  '2017-03-31T20:00:00-04:00,3,0.0'])
print("Test2 Passed!")