## To compile
mvn compile

## To run code
#mvn exec:java -Dexec.mainClass="com.ngdb.htapscheduling.Simulation"

mvn exec:java -Dexec.mainClass="com.ngdb.htapscheduling.Simulation" -Dexec.args="cluster workload scheduler mem_mgmt"
