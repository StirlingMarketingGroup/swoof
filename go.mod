module github.com/StirlingMarketingGroup/swoof

go 1.15

replace github.com/StirlingMarketingGroup/cool-mysql => ../cool-mysql

require (
	github.com/Ompluscator/dynamic-struct v1.2.0
	github.com/StirlingMarketingGroup/cool-mysql v0.0.0-20201201224208-f1f55a9171fa
	github.com/fatih/color v1.7.0
	github.com/mattn/go-colorable v0.1.2 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/pkg/errors v0.9.1
	github.com/posener/cmd v1.3.4
	github.com/vbauerster/mpb/v5 v5.3.0
	golang.org/x/sys v0.0.0-20201202213521-69691e467435 // indirect
)
