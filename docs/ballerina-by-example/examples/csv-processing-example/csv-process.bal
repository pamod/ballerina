import ballerina.data.sql;
import ballerina.io;

struct Employee {
    int id;
    string name;
    float salary;
}

public function getRecord (io:DelimitedRecordChannel recordChannel) (Employee, boolean) {
    Employee emp = {};
    boolean hasNext;
    try {
        hasNext = recordChannel.hasNextTextRecord();
        if (hasNext) {
            var fields, _ = recordChannel.nextTextRecord();
            emp.id, _ = <int>fields[0];
            emp.name = fields[1];
            emp.salary, _ = <float>fields[2];
        }
    } catch (error e) {
        throw e;
    }
    return emp, hasNext;
}

public function main (string[] args) {
    //Create in memory table constrained by the Employee struct type.
    table < Employee> employeeTable = {};
    io:ByteChannel byteChannel = io:openFile("./files/sample.csv", "r");
    io:CharacterChannel characterChannel = io:createCharacterChannel(byteChannel, "UTF-8");
    io:DelimitedRecordChannel recordChannel = io:createDelimitedRecordChannel(characterChannel, "\\r?\\n", ",");
    boolean hasRecord = true;
    Employee employee;
    while (hasRecord) {
        employee, hasRecord = getRecord(recordChannel);
        if(hasRecord){
            employeeTable.add(employee);
        }
    }
    io:print("Table Data:");
    io:println(employeeTable);
}
