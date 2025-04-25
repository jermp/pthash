
# $1 <-- .json filename without the .json extension

python3 ../script/plot_space.py -i $1.json -o $1.space.a1.0.opt.pdf -a 1.0 -b opt
python3 ../script/plot_space.py -i $1.json -o $1.space.a1.0.skew.pdf -a 1.0 -b skew

python3 ../script/plot_avg_query_time.py -i $1.json -o $1.query_time.a1.0.opt.pdf -a 1.0 -b opt
python3 ../script/plot_avg_query_time.py -i $1.json -o $1.query_time.a1.0.skew.pdf -a 1.0 -b skew

python3 ../script/plot_avg_building_time.py -i $1.json -o $1.build_time.a.1.0.opt.pdf -b opt
python3 ../script/plot_avg_building_time.py -i $1.json -o $1.build_time.a.1.0.skew.pdf -b skew
