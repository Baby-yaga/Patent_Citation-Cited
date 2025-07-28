clear
forval year = 1985/2024 {
	import delimited "E:/20250726专利数据-Rstata/二阶引证/`year'.csv", stringcols(7) clear
	bysort 公开号:egen 引证次数_2阶 = count( 引证专利_2rd )
	drop 引证专利_2rd
	duplicates drop *, force

	bysort 公开号:egen 引证次数_1阶 = count( 引证专利_1st )
	drop 引证专利_1st
	duplicates drop *, force

	keep 申请号 申请日 公开号 公开日 引证次数_1阶 引证次数_2阶
	reorder 申请号 申请日 公开号 公开日 引证次数_1阶 引证次数_2阶
	local id = `year' - 1985
	save "E:/20250726专利数据-Rstata/Result_二阶引证/result_`id'.dta", replace
}

clear
forval year = 1985/2024 {
	import delimited "E:/20250726专利数据-Rstata/二阶被引证/`year'.csv", stringcols(7) clear
	bysort 公开号:egen 被引证次数_2阶 = count( 被引证专利_2rd )
	drop 被引证专利_2rd
	duplicates drop *, force

	bysort 公开号:egen 被引证次数_1阶 = count( 被引证专利_1st )
	drop 被引证专利_1st
	duplicates drop *, force

	keep 申请号 申请日 公开号 公开日 被引证次数_1阶 被引证次数_2阶
	reorder 申请号 申请日 公开号 公开日 被引证次数_1阶 被引证次数_2阶
	local id = `year' - 1985
	save "E:/20250726专利数据-Rstata/Result_二阶被引证/result_`id'.dta", replace
}