# arg1 - results folder
mkdir results
mkdir results/$1

index=0
for beta in 0.0 1.0
do
	for alpha in $(seq 0.0 0.1 1.0) # Variation of alpha
	do
        sched_args=`echo "macro_cluster ssb scheduler_a"$alpha"_b"$beta" two_level"`
        echo $sched_args
        echo $index
	taskset -c $index mvn exec:java -Dexec.mainClass="com.ngdb.htapscheduling.Simulation" -Dexec.args="$sched_args" > results/$1/output_a"$alpha"_b"$beta"&
	((index+=1))
	done
done

for alpha in 0.0 1.0
do
        for beta in $(seq 0.1 0.1 0.9) # Variation of alpha
        do
        sched_args=`echo "macro_cluster ssb scheduler_a"$alpha"_b"$beta" two_level"`
        echo $sched_args
        echo $index
        taskset -c $index mvn exec:java -Dexec.mainClass="com.ngdb.htapscheduling.Simulation" -Dexec.args="$sched_args" > results/$1/output_a"$alpha"_b"$beta"&
        ((index+=1))
        done
done
