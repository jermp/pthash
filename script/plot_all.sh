
# $1 <-- .json filename without the .json extension

python3 plot_building_time.py -i $1.json -o $1.build_time.skew.pdf -b skew
python3 plot_building_time.py -i $1.json -o $1.build_time.opt.pdf -b opt

python3 plot_query_time.py -i $1.json -o $1.query_time.skew.a0.94.pdf -b skew -a 0.94
python3 plot_query_time.py -i $1.json -o $1.query_time.opt.a0.94.pdf -b opt -a 0.94

python3 plot_space.py -i $1.json -o $1.space.opt.a0.94.pdf -b opt -a 0.94
python3 plot_space.py -i $1.json -o $1.space.skew.a0.94.pdf -b skew -a 0.94
