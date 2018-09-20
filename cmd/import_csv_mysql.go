package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mtools "github.com/everywan/import-csv-to-mysql"
	"github.com/jinzhu/gorm"
	"github.com/spf13/cobra"
)

/*
	问题: 重复导入 是否需要检测?(涉及到状态)
	csv格式: `phone,balance,expired_at`
	时间格式: `2018-08-17 15:05:46`
*/

var csvDataFile string
var wg sync.WaitGroup
var resultChan chan bool

var importCSVCmd = &cobra.Command{
	Use:   "import",
	Short: "导入csv到mysql",
	Long:  `处理csv导入文件, 将数据导入到mysql中. 使用 --csv 选项指定数据源. 注意, 为保证数据库中原数据的准确性, 程序会先检查csv文件是否包含错误格式, 只有当csv文件中没有错误时, 才会更新到数据库中.`,
	Run: func(cmd *cobra.Command, args []string) {
		resultChan = make(chan bool, 1)
		start := time.Now().UnixNano()
		opts := new(mtools.ApplicationOptions)
		opts.Load()
		merchants, sum, err := checkAndReadCSV(csvDataFile)
		if err != nil {
			fmt.Printf("请检查csv数据格式\n")
			return
		}
		fmt.Printf("共读取 %d 行记录, 格式全部检查正确\n", sum)
		go importToDB(opts.Database, merchants)
		select {
		case result := <-resultChan:
			if result == true {
				end := time.Now().UnixNano()
				fmt.Printf("共成功导入 %d 行记录, 用时 %d 毫秒\n", sum, (end-start)/1000000)
			} else {
				end := time.Now().UnixNano()
				fmt.Printf("类型检查正确, 导入数据库失败. 共有 %d 行记录, 用时 %d 毫秒\n", sum, (end-start)/1000000)
			}
		}
	},
}

// 检查+读取csv文件
func checkAndReadCSV(path string) (merchants []mtools.ExampleCSV, sum int, err error) {
	// 建立连接
	csvFile, err := os.Open(path)
	if err != nil {
		fmt.Printf("open %s failed, err: %s\n", path, err)
		return
	}
	defer csvFile.Close()

	// 逐行检查+读取, sum 只作为行数记录. 为方便阅读, 从1开始
	sum = 0
	br := bufio.NewReader(csvFile)
	haveNextLine := true
	for haveNextLine {
		line, err := br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				haveNextLine = false
			} else {
				fmt.Printf("read %s failed, line: %d, err: %s\n", csvDataFile, sum, err)
				return nil, 0, err
			}
		}
		merchant, err := cleanCSVLine(line)
		merchants = append(merchants, merchant)
		if err != nil {
			fmt.Printf("read %s failed,line: %d, merchant: %+v, err: %s\n", csvDataFile, sum, merchant, err)
			return nil, 0, err
		}
		sum++
	}
	return merchants, sum, nil
}

/*清理异常情况, 并打印行数, 退出执行:1. 空行2. 字段缺少3. 字段太多*/
func cleanCSVLine(line string) (merchant mtools.ExampleCSV, err error) {
	if len(line) < 6 {
		return merchant, fmt.Errorf("缺少字段")
	}
	// 去除 \r \n
	line = strings.Replace(line, "\n", "", -1)
	line = strings.Replace(line, "\r", "", -1)
	fields := strings.Split(line, ",")
	if len(fields) != 3 {
		return merchant, fmt.Errorf("字段数量不对")
	}
	// 检查字段类型
	phone, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return merchant, fmt.Errorf("字段类型 phone 错误, 字段值: %s", fields[0])
	}
	balance, _ := strconv.ParseInt(fields[1], 10, 32)
	if err != nil {
		return merchant, fmt.Errorf("字段类型 balance 错误, 字段值: %s", fields[1])
	}
	// 检查 ExpiredAt(fields[2]) 是否是时间戳格式
	if _, err := time.Parse("2006-01-02 15:04:05", fields[2]); err != nil {
		return merchant, fmt.Errorf("字段类型 expired_at 错误, 字段值: %s", fields[2])
	}

	return mtools.ExampleCSV{
		MobilePhone: phone,
		Balance:     int(balance),
		ExpiredAt:   fields[2],
		Status:      mtools.ExampleCSVStatusUnBind,
	}, nil
}

// 导入到数据库
func importToDB(dbOptions mtools.DatabaseOptions, merchants []mtools.ExampleCSV) {
	// 建立数据库链接
	db, err := mtools.NewDatabase(dbOptions)
	if err != nil {
		fmt.Printf("init %s failed, err: %s", "could not connect database\n", err)
		return
	}
	defer db.Close()
	tx := db.Begin()
	index := 0
	mLength := len(merchants)
	for {
		wg.Add(1)
		if index+10000 < mLength {
			go func(index int) {
				batchImport(tx, merchants[index:index+10000])
			}(index)
		} else {
			go func(index int) {
				batchImport(tx, merchants[index:mLength])
			}(index)
			break
		}
		index = index + 10000
	}
	wg.Wait()
	if tx.Commit().Error != nil {
		resultChan <- false
		return
	}
	resultChan <- true
}

// 合并导入数据
func batchImport(tx *gorm.DB, merchants []mtools.ExampleCSV) {
	defer wg.Done()
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("插入数据库出错, 错误是: %v.\n", err)
			tx.Rollback()
		}
	}()
	sql := fmt.Sprintf("INSERT into %s(business_account_id,mobile_phone,balance,expired_at,status) VALUES ", mtools.ExampleCSV{}.TableName())

	for _, m := range merchants {
		value := fmt.Sprintf("(%d, %d, %d,'%s',%d),", m.BusinessAccountID, m.MobilePhone, m.Balance, m.ExpiredAt, m.Status)
		sql = strings.Join([]string{sql, value}, "")
	}
	// 替换最后一个 `,` 为 `;`
	sql = sql[0:len(sql)-1] + ";"

	err := tx.Exec(sql).Error
	if err != nil {
		tx.Rollback()
		resultChan <- false
	}
}

func init() {
	RootCmd.AddCommand(importCSVCmd)
	importCSVCmd.PersistentFlags().StringVarP(&csvDataFile, "csv", "", "", "config file (default is ./data.csv)")
}
