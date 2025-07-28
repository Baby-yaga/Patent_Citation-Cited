forvalues year = 2023(1)2024{
	cd "E:/20250726专利数据-Rstata/专利引证/"
	qui describe using "`year'.dta"
	local total_rows = r(N)
	display "Total rows in the dataset: " `total_rows'
	* 设置每个子文件的行数
	local chunk_size = 50000
	* 循环分割数据
	forval i = 1(1) `=floor(`total_rows' / `chunk_size')' {
		* 计算起始行和结束行
		local start_row = (`i' - 1) * `chunk_size' + 1
		local end_row = `i' * `chunk_size'
		* 确保最后一部分处理余下的行
		if `end_row' > `total_rows' {
			local end_row = `total_rows'
		}

		* 加载数据并保存为新文件
		cd "E:/20250726专利数据-Rstata/专利引证/"
		use in `start_row'/`end_row' using "`year'.dta", clear
		
		local path "E:/20250726专利数据-Rstata/专利引证-拆分版/`year'/"
		capture mkdir "`path'"
		cd "`path'"
		save "part`i'.dta", replace
	}
	* 处理最后剩余的部分，如果还有剩余行
	if `total_rows' > `chunk_size' * `=floor(`total_rows' / `chunk_size')' {
		local start_row = `chunk_size' * `=floor(`total_rows' / `chunk_size')' + 1
		cd "E:/20250726专利数据-Rstata/专利引证/"
		use in `start_row'/`total_rows' using "`year'.dta", clear
		local path "E:/20250726专利数据-Rstata/专利引证-拆分版/`year'/"
		capture mkdir "`path'"
		cd "`path'"
		local part_num = floor(`total_rows' / `chunk_size') + 1
		save "part`part_num'.dta", replace
	}
}