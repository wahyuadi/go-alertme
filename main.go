package main

import (
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Rhymen/go-whatsapp"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gomodule/redigo/redis"
	"github.com/skip2/go-qrcode"
	"github.com/ulule/deepcopier"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	waConnTimeout  = 15
	waitBeforeQuit = 86400 // seconds before we quit
	recipientJid   = "6288888888888"
	testGroupJid   = "6288888888888-1509928132@g.us" // Whatsapp Group JID

	redisServer          = "127.0.0.1"
	redisPort            = "6379"
	redisAlertMessageKey = "alert-message"
	redisAlertSettingKey = "alert-setting"
	redisPollInterval    = 1

	sessionDir  = "/tmp/"
	sessionFile = sessionDir + "whatsappSession.gob"
	filterFile  = sessionDir + "whatsappFilter.gob"

	dbDriver = "mysql"

	dbHostWrite = "server"
	dbUserWrite = "user"
	dbPassWrite = "secret"
	dbNameWrite = "dbname"
	dbPortWrite = "3306"

	// as the name suggest, do not make changes to *Read
	dbHostRead = "server"
	dbUserRead = "user"
	dbPassRead = "secret"
	dbNameRead = "dbname"
	dbPortRead = "3306"

	mysqlDateFormat = "%d-%02d-%02d %02d:%02d:%02d"
	globalAlertFile = "global.json"

	maxConcurrent = 4
	bufferSize    = 100000
)

var (
	wac         *whatsapp.Conn
	wah         *waHandler
	dbRead      *sql.DB
	dbWrite     *sql.DB
	redisClient redis.Conn
	redisPool   *redis.Pool

	wg      sync.WaitGroup
	errChan chan error

	projects map[int]string // map of projectId to projectTitle / projectName

	sender string
)

type waHandler struct {
	c *whatsapp.Conn
}

type AlertSetting struct {
	ProjectId         int             `json:"project_id"`
	Recurring         int             `json:"recurring"`
	RecurringInterval int             `json:"recurring_interval"`
	MaxRecurring      int             `json:"max_recurring"`
	AlertTypeId       int             `json:"alert_type_id"`
	AlertCode         string          `json:"alert_code"`
	Recipients        string          `json:"recipients"`
	MinThreshold      sql.NullFloat64 `json:"min_threshold"`
	MaxThreshold      sql.NullFloat64 `json:"max_threshold"`
	Active            int             `json:"is_active"`
	Scope             string          `json:"scope"`
}

func (setting *AlertSetting) SendAlert(wac *whatsapp.Conn, id int) {

	log.Println(fmt.Sprintf("processAlertSetting: Processing ID %v for metrics %v (scope: %v)", setting.ProjectId, setting.AlertCode, setting.Scope))
	category := getAlertCategory(setting.AlertCode)
	code := setting.AlertCode

	fmt.Println(fmt.Sprintf("processAlertSetting: AlertCode '%v' gives '%v'", setting.AlertCode, category))

	if strings.Contains(code, "sentiment_mention") {
		fmt.Println(fmt.Sprintf("Consumer[%v] Processing Sentiment Alert", id))
		alertSentiment(wac, setting)
	}
	if strings.Contains(code, "keyword_efficiency") {
		fmt.Println(fmt.Sprintf("Consumer[%v] Processing Keyword Efficiency Alert", id))
		alertKeywordEfficiency(wac, setting)
	}
	if strings.Contains(code, "bot_suspect") {
		fmt.Println(fmt.Sprintf("Consumer[%v] Processing Bot Suspect Alert", id))
		alertBotSuspect(wac, setting)
	}
	if strings.Contains(code, "twitter_doc") {
		fmt.Println(fmt.Sprintf("Consumer[%v] Processing Twitter Doc Count Alert", id))
		alertSolrDocCount(wac, setting, "twitter")
	}
}

//HandleError needs to be implemented to be a valid WhatsApp handler
func (wah *waHandler) HandleError(err error) {
	// we use errChan to ensure error handling is blocking until it finishes
	errChan <- err
	log.Printf("[ERROR] error occured: %v\n", err)
}

//
// Modified from https://play.golang.org/p/t4z-f8I-ih
// Treat input string as a rune; All Unicode code points should display properly
//
func runify(s string) string {
	result := ""
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		result += fmt.Sprintf("%c", runes[i])
	}
	return result
}

// read session from existing gob saved session file
func readSession() (whatsapp.Session, error) {

	session := whatsapp.Session{}
	savedSession := sessionFile

	// if savedSession file exists
	if _, err := os.Stat(savedSession); err == nil {
		file, err := os.Open(savedSession)
		defer file.Close()
		if err != nil {
			checkError(err, "os.Open", false, false)
			return session, err
		}
		fmt.Println("Reading session from ", sessionFile)
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(&session)
		if err != nil {
			checkError(err, "decoder.Decode", false, false)
			return session, err
		}
		return session, nil
	} else {
		return session, err
	}
}

func writeSession(session whatsapp.Session) error {
	file, err := os.Create(sessionFile)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(session)
	if err != nil {
		return err
	}
	return nil
}

// create WA client connections and perform login
func waInit() (*whatsapp.Conn, *waHandler, error) {
	// create new whatsapp connection
	wac, err := whatsapp.NewConn(waConnTimeout * time.Second)
	if err != nil {
		log.Printf("%v", err)
		fmt.Fprintf(os.Stderr, "error creating connection: %v\n", err)
		return nil, nil, err
	}

	//Add handler and attach whatsapp.Conn to it
	wah = &waHandler{c: wac}
	wac.AddHandler(wah)
	//wac.AddHandler(&waHandler{})

	// attempt to login. QR code will show up here
	err = login(wac)
	if err != nil {
		log.Printf("%v", err)
		fmt.Fprintf(os.Stderr, "error logging in: %v\n", err)
		return nil, nil, err
	}
	return wac, wah, nil
}

// modified from https://golangme.com/blog/how-to-use-redis-with-golang/
func newRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:   80,
		MaxActive: 12000, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisServer+":"+redisPort)
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

//
// Get value from given Redis key
// If we don't have one, try to get from DB. If succeed, update the Redis cache for given key
//
func getRedisKey(key string) string {
	var temp interface{}
	result := ""

	if key == "" {
		log.Printf("[REDIS] getRedisKey: empty key forbidden")
		return ""
	}

	redisClient := redisPool.Get()
	defer redisClient.Close()

	redisType, err := redis.String(redisClient.Do("TYPE", key))
	checkError(err, "getRedisKey: redisClient.Do TYPE", false, false)

	if redisType == "string" {
		temp, err = redisClient.Do("GET", key)
		checkError(err, fmt.Sprintf("getRedisKey: redisClient.Do GET key '%v'", key), true, true)
	} else {
		// for now, we consider other than string data as redis list
		temp, err = redisClient.Do("RPOP", key)
	}

	if temp != nil {
		if v, ok := temp.(string); ok {
			result = runify(v)
			log.Printf(fmt.Sprintf("[REDIS] getRedisKey: key '%v' gives '%v'", key, result))
		} else {
			// We may have cases where value is []uint8, where each member is an ASCII decimal code
			var text []string

			for _, v := range temp.([]uint8) {
				text = append(text, fmt.Sprintf("%s", string(v)))
			}
			result = runify(strings.Join(text, ""))

			log.Printf(fmt.Sprintf("[REDIS] getRedisKey: key '%v' gives results in type '%T': '%v'", key, temp, result))
		}
	} else {
		log.Println(fmt.Sprintf("getRedisKey(%v) returns NIL ", key))
		return ""
	}
	return runify(result)
}

// send error message to admin via WA
// STILL TESTING: send to multiple recipients
func sendMessage(wac *whatsapp.Conn, to, prefix, msg string) error {

	var recipients []string

	if strings.Contains(to, ",") {
		recipients = strings.Split(to, ",")
	} else {
		recipients = append(recipients, to)
	}

	for _, recipient := range recipients {
		// strip anything after '@'; will append later depending on recipients
		if strings.Contains(to, "@") {
			re := regexp.MustCompile("@.*$")
			recipient = re.ReplaceAllString(to, "")
		}

		if strings.Contains(recipient, "-") {
			recipient += "@g.us" // WA group
		} else {
			recipient += "@s.whatsapp.net" // WA user
		}

		text := whatsapp.TextMessage{
			Info: whatsapp.MessageInfo{
				RemoteJid: recipient,
			},
			Text: prefix + " " + msg,
		}

		if wac == nil {
			return errors.New("sendmessage has empty WA connection")
			os.Exit(1)
		} else {
			s, err := wac.Send(text)
			if err != nil {
				checkError(err, "wac.Send", false, false)
				return err
				//return errors.New(fmt.Sprintf("Got response '%v' from sendMessage failed to send %v this message: %v", s, recipient, msg))
			} else {
				log.Println(fmt.Sprintf("[INFO] wac.Send to %v returns '%v'.\n %v", recipient, s, err))
			}
		}
	}

	return nil
}

// establish connection to DB and returns db connector variable
func dbConnWrite() (db *sql.DB) {

	connString := dbUserWrite + ":" + dbPassWrite + "@tcp(" + dbHostWrite + ":" + dbPortWrite + ")/" + dbNameWrite
	//fmt.Println("Connection string: ", connString)

	db, err := sql.Open(dbDriver, connString)
	if err != nil {
		panic(err.Error())
	}
	// fix 'closing bad idle connection: EOF' error
	db.SetMaxIdleConns(0)

	return db
}

// establish connection to DB and returns db connector variable
func dbConnRead() (db *sql.DB) {

	connString := dbUserRead + ":" + dbPassRead + "@tcp(" + dbHostRead + ":" + dbPortRead + ")/" + dbNameRead
	//fmt.Println("Connection string: ", connString)

	db, err := sql.Open(dbDriver, connString)
	if err != nil {
		panic(err.Error())
	}
	// fix 'closing bad idle connection: EOF' error
	db.SetMaxIdleConns(0)

	return db
}

func getProject() map[int]string {
	var projectId int
	var projectTitle string

	result := make(map[int]string)

	sqlStr := `SELECT id, title FROM project WHERE status = 'running' ORDER BY id ASC`
	rows, err := dbRead.Query(sqlStr)
	checkError(err, "getProjectIds: dbRead.Query", false, false)

	for rows.Next() {
		if err := rows.Scan(&projectId, &projectTitle); err != nil {
			checkError(err, "getProjectIds: rows.Scan", false, false)
		} else {
			result[projectId] = projectTitle
		}
	}
	return result
}

func getProjectIds(projects map[int]string) []int {
	var result []int
	for k := range projects {
		result = append(result, k)
	}
	return result
}

func getProjectNames(projects map[int]string) []string {
	var result []string
	for _, v := range projects {
		result = append(result, v)
	}
	return result
}

func formatTimestampSigned(s string, t int64, isUTC bool) string {
	tm := time.Unix(t, 0)
	var date string

	if isUTC {
		date = fmt.Sprintf(s, tm.UTC().Year(), tm.UTC().Month(), tm.UTC().Day(), tm.UTC().Hour(), tm.UTC().Minute(), tm.UTC().Second())
	} else {
		date = fmt.Sprintf(s, tm.Year(), tm.Month(), tm.Day(), tm.Hour(), tm.Minute(), tm.Second())
	}

	//fmt.Println(t)

	return date
}

func checkError(e error, context string, isFatal bool, notify bool) {
	if e != nil {
		msg := fmt.Sprintf("Got error in this context: %v", context)
		fmt.Println(msg)
		fmt.Println(e)
		msg += e.Error()
		if notify {
			if wac != nil {
				err := sendMessage(wac, recipientJid, "[ERROR]", msg)
				log.Println(err)
			} else {
				log.Println(fmt.Sprintf("[DEBUG] checkError cannot notify '%v' this message: '%v' because wac is '%v'l", recipientJid, msg))
				fmt.Println(fmt.Sprintf("[DEBUG] checkError cannot notify '%v' this message: '%v' because wac is '%v'l", recipientJid, msg))
			}
		}
		if isFatal {
			panic(e)
		}
	}
}

func login(wac *whatsapp.Conn) error {
	//load saved session
	session, err := readSession()

	if err == nil {
		//restore session
		session, err = wac.RestoreWithSession(session)
		if err != nil {
			e := fmt.Sprintf("restoring failed: %v\n", err.Error())
			//if wac != nil {
			//	err = sendMessage(wac, recipientJid, e)
			//	checkError(err, "sendMessage within login", false, false)
			//}
			return fmt.Errorf(e)
		}
	} else {
		//no saved session -> regular login
		qr := make(chan string)

		go func() {
			// I have yet to find way to decrease this qr code size therefore
			// we use qrcode.QRCode and its 'toSmallString' method instead.
			// It exists for reference only.
			//terminal := qrcodeTerminal.New()
			//terminal.Get(<-qr).Print()
			var q *qrcode.QRCode

			q, err := qrcode.New(<-qr, qrcode.Low)
			fmt.Println(q)

			checkError(err, "qrcode.New", false, false)
			println(q.ToSmallString(false))
		}()

		session, err = wac.Login(qr)
		if err != nil {
			return fmt.Errorf("error during login: %v\n", err)
		}

		sender = session.Wid
	}

	//save session
	err = writeSession(session)
	if err != nil {
		return fmt.Errorf("error saving session: %v\n", err)
	}
	return nil
}

func classToSentimentName(class string) string {
	switch class {
	case "0":
		return "Negative"
	case "1":
		return "Positive"
	case "2":
		return "Neutral"
	default:
		return "New"
	}
}

func getAlertSetting() []AlertSetting {

	//var allAlerts, globalAlerts, projectAlerts []AlertSetting
	var allAlerts []AlertSetting
	var projectId, alertTypeId, recurring, recurringInterval, maxRecurring, isActive int
	var alertCode, recipients, scope string
	var minThreshold, maxThreshold sql.NullFloat64

	allAlerts = []AlertSetting{}
	//globalAlerts = []AlertSetting{}
	//projectAlerts = []AlertSetting{}

	alertSettingSQL := `SELECT s.project_id, s.alert_type_id, t.code, s.min_threshold, s.max_threshold, s.recurring, 
		s.recurring_interval, s.max_recurring, s.recipients, s.is_active, s.scope FROM alert_rule_setting s 
		JOIN alert_type t ON s.alert_type_id = t.id 
		JOIN project p ON s.project_id = p.id
		WHERE s.is_active = '1' AND p.status = 'running' ORDER BY s.recurring_interval ASC `

	rows, err := dbWrite.Query(alertSettingSQL)
	checkError(err, "db.Query(alertSettingSQL", false, false)

	for rows.Next() {
		setting := AlertSetting{}

		if err := rows.Scan(&projectId, &alertTypeId, &alertCode, &minThreshold, &maxThreshold, &recurring, &recurringInterval, &maxRecurring, &recipients, &isActive, &scope); err != nil {
			checkError(err, "alertSettings.Scan", false, false)
		} else {
			setting.ProjectId = projectId
			setting.AlertTypeId = alertTypeId
			setting.AlertCode = alertCode
			setting.MinThreshold = minThreshold
			setting.MaxThreshold = maxThreshold
			setting.Recurring = recurring
			setting.RecurringInterval = recurringInterval
			setting.MaxRecurring = maxRecurring
			setting.Recipients = recipients
			setting.Active = isActive
			setting.Scope = scope

			allAlerts = append(allAlerts, setting)
		}
	}
	return allAlerts
}

func pushAlertSetting(settings []AlertSetting) {

	// map of alert_code to map of project ID to empty struct
	// our goal is to get unique alert_code; each of which contain unique projectID
	// so that each alert rule will only trigger one notification
	alertCodes := make(map[string]map[string]struct{})
	projectIds := getProjectIds(projects)

	//go func(s []AlertSetting, c chan *[]byte) {
	populate := func(s []AlertSetting) {

		redisClient = redisPool.Get()
		defer redisClient.Close()

		// build projectAlerts list first; they're the special case we need to keep
		for _, setting := range s {
			if strings.ToLower(setting.Scope) == "project" {
				pid := strconv.Itoa(setting.ProjectId)
				if code, ok := alertCodes[setting.AlertCode]; ok {
					if _, ok := code[pid]; ok {
						continue
					} else {
						code[pid] = struct{}{}
					}
				} else {
					alertCodes[setting.AlertCode] = make(map[string]struct{})
					alertCodes[setting.AlertCode][pid] = struct{}{}
				}

				settingByte, err := json.MarshalIndent(setting, "", " ")
				checkError(err, "json.MarshalIndent setting", true, false)
				//fmt.Println(fmt.Sprintf("pushAlertSetting: settingByte '%v'", settingByte))
				settingStr := string(settingByte)
				fmt.Println(fmt.Sprintf("pushALertSetting: settingStr %v", settingStr))

				r, err := redisClient.Do("LPUSH", redisAlertSettingKey, settingStr)
				if err != nil {
					redisClient = redisPool.Get()
					r, err = redisClient.Do("LPUSH", redisAlertSettingKey, settingStr)
				}
				checkError(err, "LPUSH redisAlertSettingKey settingStr", true, false)
				log.Println(fmt.Sprintf("r -> %v", r))
			}
		}
		fmt.Println("projectAlerts built.")

		for _, setting := range settings {
			if strings.ToLower(setting.Scope) == "global" {
				for _, id := range projectIds {
					newAlert := AlertSetting{}
					deepcopier.Copy(&setting).To(&newAlert)

					pid := strconv.Itoa(id)
					if code, ok := alertCodes[setting.AlertCode]; ok {
						if _, ok := code[pid]; ok {
							continue
						} else {
							code[pid] = struct{}{}
							newAlert.ProjectId = id
						}
					} else {
						temp := make(map[string]struct{})
						temp[pid] = struct{}{}
						alertCodes[setting.AlertCode] = temp
						newAlert.AlertCode = setting.AlertCode
						newAlert.ProjectId = id
					}

					settingByte, err := json.MarshalIndent(newAlert, "", " ")
					checkError(err, "json.MarshalIndent setting", true, false)
					//fmt.Println(fmt.Sprintf("pushAlertSetting: settingByte '%v'", settingByte))
					settingStr := string(settingByte)
					fmt.Println(fmt.Sprintf("pushALertSetting: settingStr %v", settingStr))

					r, err := redisClient.Do("LPUSH", redisAlertSettingKey, settingStr)
					if err != nil {
						redisClient = redisPool.Get()
						r, err = redisClient.Do("LPUSH", redisAlertSettingKey, settingStr)
					}
					checkError(err, "LPUSH redisAlertSettingKey settingStr", true, false)
					log.Println(fmt.Sprintf("r -> %v", r))
				}
			}
		}
		fmt.Println("globalAlerts built.")
	}
	populate(settings)
}

//func saveAlertMessage(setting AlertSetting, message string, alertMessageChan chan *string) {
func saveAlertMessage(wac *whatsapp.Conn, setting *AlertSetting, message string) {

	redisClient = redisPool.Get()
	defer redisClient.Close()

	sql := "INSERT INTO alert_message_log (project_id, alert_type_id, message, sender, recipient, date_sent) VALUES (?, ?, ?, ?, ?, ?)"
	insertAlert, err := dbWrite.Prepare(sql)
	checkError(err, "dbWrite.Prepare", false, false)
	defer insertAlert.Close()

	if sender == "" {
		sender = "me"
	}
	_, err = insertAlert.Exec(setting.ProjectId, setting.AlertTypeId, message, sender, setting.Recipients, formatTimestampSigned(mysqlDateFormat, time.Now().Unix(), true))
	if err != nil {
		checkError(err, "insertAlert.Exec", false, false)
	} else {
		temp := strings.Split(message, "|")
		to, msg := temp[0], temp[1]
		if to != "" {
			err := sendMessage(wac, to, "", msg)
			checkError(err, "sendMessage", false, false)
		} else {
			err := sendMessage(wac, recipientJid, "[Missing recipient JID]", msg)
			checkError(err, "sendMessage", false, false)
		}
	}
}

func getAlertCategory(code string) string {
	result := ""
	if strings.Contains(code, "sentiment_mention") {
		result = "sentiment"
	}
	if strings.Contains(code, "keyword_efficiency") {
		result = "keyword_efficiency"
	}
	if strings.Contains(code, "bot_suspect") {
		result = "bot_suspect"
	}
	if strings.Contains(code, "twitter_doc") {
		result = "twitter_doc"
	}
	return result
}

func tableExists(dbName, tableName string) bool {
	var c int

	sqlStr := `SELECT COUNT(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '?') AND (TABLE_NAME = '?')`
	err := dbRead.QueryRow(sqlStr, dbName, tableName).Scan(&c)
	checkError(err, "tableExists: dbRead.QueryRow", false, false)

	if c > 0 {
		return true
	} else {
		return false
	}
}

//func alertSentiment(setting AlertSetting, alertMessageChan chan struct{}) {
func alertSentiment(wac *whatsapp.Conn, setting *AlertSetting) {

	var projectId, totalMentions, classMentions int
	var ratio float64
	var prefix, code, tableName, projectName, class, sentimentClass, totalPerClass string

	projectId = setting.ProjectId
	projectName = ""
	totalMentions = 0
	classMentions = 0
	ratio = 0.0
	class = ""
	sentimentClass = ""
	totalPerClass = ""
	tableName = "mentions_" + strconv.Itoa(projectId)
	code = setting.AlertCode

	if tableExists(dbNameRead, tableName) {

		prefix = "[Sentiment]"
		if strings.Contains(strings.ToLower(code), "negative") {
			class = "0"
		}
		if strings.Contains(strings.ToLower(code), "positive") {
			class = "1"
		}
		if strings.Contains(strings.ToLower(code), "neutral") {
			class = "2"
		}

		sqlStr := `SELECT class_sentiment, COUNT(*) as size FROM %v GROUP BY class_sentiment ORDER BY class_sentiment`
		rows, err := dbRead.Query(fmt.Sprintf(sqlStr, tableName))
		checkError(err, "db.Query(sqlStr, tableName, class)", false, false)

		for rows.Next() {
			if e := rows.Err(); e != nil {
				log.Println(fmt.Sprintf("rows.Next() got this error:\n%v", e))
				break
			}
			projectName = projects[projectId]
			if err := rows.Scan(&sentimentClass, &totalPerClass); err != nil {
				checkError(err, "rows.Scan(&projectName, &sentimentClass, &totalPerClass)", false, false)
			} else {
				sum, err := strconv.Atoi(totalPerClass)
				checkError(err, "strconv.Atoi(totalClass)", false, false)
				totalMentions += sum

				if class == sentimentClass {
					classMentions = sum
				}
			}
		}

		ratio = float64(classMentions) / float64(totalMentions)

		if strings.Contains(code, "max_") {
			if setting.MaxThreshold.Valid {
				if ratio > setting.MaxThreshold.Float64 {
					alertStr := fmt.Sprintf("%v|%v Project ID %v ('%v') exceeds maximum '%v' sentiment ratio threshold ('%v' > '%v')", setting.Recipients, prefix, projectId, projectName, classToSentimentName(class), ratio, setting.MaxThreshold.Float64)
					saveAlertMessage(wac, setting, alertStr)
					checkError(err, alertStr, false, false)
				}
			}
		}

		if strings.Contains(code, "min_") {
			if setting.MinThreshold.Valid {
				if ratio < setting.MinThreshold.Float64 {
					alertStr := fmt.Sprintf("%v|%v Project ID %v ('%v') drops below minimum '%v' sentiment ratio threshold ('%v' < '%v')", setting.Recipients, prefix, projectId, projectName, classToSentimentName(class), ratio, setting.MinThreshold.Float64)
					saveAlertMessage(wac, setting, alertStr)
					checkError(err, alertStr, false, false)
				}
			}
		}
	}
}

//func alertKeywordEfficiency(setting AlertSetting, alertMessageChan chan struct{}) {
func alertKeywordEfficiency(wac *whatsapp.Conn, setting *AlertSetting) {

	var prefix, code, projectName string
	var projectId int
	var keywordEfficiency, ratio float64

	projectId = setting.ProjectId
	code = setting.AlertCode
	keywordEfficiency = 0.0
	projectName = ""
	prefix = "[Keyword Efficiency]"

	sqlStr := `SELECT ratio FROM keyword_efficiency WHERE project_id = ? ORDER BY id DESC LIMIT 1`

	err := dbRead.QueryRow(sqlStr, projectId).Scan(&keywordEfficiency)
	checkError(err, "dbRead.QueryRow", false, false)

	ratio = keywordEfficiency
	projectName = projects[projectId]

	if strings.Contains(code, "max_") {
		if setting.MaxThreshold.Valid {
			if ratio > setting.MaxThreshold.Float64 {
				alertStr := fmt.Sprintf("%v|%v Project ID %v ('%v') exceeds maximum keyword efficiency threshold ('%v' > '%v')", setting.Recipients, prefix, projectId, projectName, ratio, setting.MaxThreshold.Float64)
				saveAlertMessage(wac, setting, alertStr)
				checkError(err, alertStr, false, false)
			}
		}
	}

	if strings.Contains(code, "min_") {
		if setting.MinThreshold.Valid {
			if ratio < setting.MinThreshold.Float64 {
				alertStr := fmt.Sprintf("%v|%v Project ID %v ('%v') drops below minimum keyword efficiency threshold ('%v' < '%v')", setting.Recipients, prefix, projectId, projectName, ratio, setting.MinThreshold.Float64)
				saveAlertMessage(wac, setting, alertStr)
				checkError(err, alertStr, false, false)
			}
		}
	}
}

//func alertBotSuspect(setting AlertSetting, alertMessageChan chan struct{}) {
func alertBotSuspect(wac *whatsapp.Conn, setting *AlertSetting) {

	var prefix, code, tableName, projectName string
	var projectId, totalAuthor int
	var ratio float64
	var botSuspect sql.NullFloat64

	code = setting.AlertCode
	projectId = setting.ProjectId
	tableName = "mentions_" + strconv.Itoa(projectId)
	totalAuthor = 0
	botSuspect = sql.NullFloat64{}
	ratio = 0.0
	projectName = ""
	prefix = "[Bot Suspect]"

	if tableExists(dbNameRead, tableName) {

		sqlStr := `SELECT COUNT(m.id) AS total, SUM(IF(b.display_scores_universal > 3.0,1,0)) AS bot_most_likely FROM %v m
					JOIN bot_score b on m.author_id = b.user_id WHERE m.media_type_id = '5'`

		err := dbRead.QueryRow(fmt.Sprintf(sqlStr, tableName)).Scan(&totalAuthor, &botSuspect)
		checkError(err, "dbRead.QueryRow", false, false)

		projectName = projects[projectId]

		if botSuspect.Valid {
			ratio = float64(botSuspect.Float64) / float64(totalAuthor)
		} else {
			ratio = 0.0
		}

		if strings.Contains(code, "max_") {
			if setting.MaxThreshold.Valid {
				if ratio > setting.MaxThreshold.Float64 {
					alertStr := fmt.Sprintf("%v|%v Project ID %v ('%v') exceeds maximum bot_suspect ratio threshold ('%v' > '%v')", setting.Recipients, prefix, projectId, projectName, ratio, setting.MaxThreshold.Float64)
					saveAlertMessage(wac, setting, alertStr)
					checkError(err, alertStr, false, false)
				}
			}
		}

		if strings.Contains(code, "min_") {
			if setting.MinThreshold.Valid {
				if ratio < setting.MinThreshold.Float64 {
					alertStr := fmt.Sprintf("%v|%v Project ID %v ('%v') drops below minimum bot_suspect ratio threshold ('%v' < '%v')", setting.Recipients, prefix, projectId, projectName, ratio, setting.MinThreshold.Float64)
					saveAlertMessage(wac, setting, alertStr)
					checkError(err, alertStr, false, false)
				}
			}
		}
	}
}

//func alertSolrDocCount(setting AlertSetting, mediaType string, alertMessageChan chan struct{}) {
func alertSolrDocCount(wac *whatsapp.Conn, setting *AlertSetting, mediaType string) {

	var docCount float64
	var prefix, code string

	docCount = 0.0
	code = setting.AlertCode
	prefix = "[Doc Count]"

	// For Solr document count by media type
	if mediaType == "twitter" {

		sqlStr := `SELECT solr_doc_count FROM media_types_stats WHERE media_type_id = '5' ORDER BY id DESC LIMIT 1`
		err := dbRead.QueryRow(sqlStr).Scan(&docCount)
		checkError(err, "dbRead.QueryRow", false, false)

		if strings.Contains(code, "max_") {
			if setting.MaxThreshold.Valid {
				if docCount > setting.MaxThreshold.Float64 {
					alertStr := fmt.Sprintf("%v|%v Media Type ID 5 ('Twitter') exceeds maximum Solr document count ('%v' > '%v')", setting.Recipients, prefix, docCount, setting.MaxThreshold.Float64)
					saveAlertMessage(wac, setting, alertStr)
					checkError(err, alertStr, false, false)
				}
			}
		}

		if strings.Contains(code, "min_") {
			if setting.MinThreshold.Valid {
				if docCount < setting.MinThreshold.Float64 {
					alertStr := fmt.Sprintf("%v|%v Media Type ID 5 ('Twitter') drops below minimum Solr document count ('%v' > '%v')", setting.Recipients, prefix, docCount, setting.MinThreshold.Float64)
					saveAlertMessage(wac, setting, alertStr)
					checkError(err, alertStr, false, false)
				}
			}
		}
	}
}

func writeSetting(setting []AlertSetting, fileName string) {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	checkError(err, fmt.Sprintf("os.OpenFile %v", fileName), false, false)
	defer file.Close()

	settingByte, err := json.MarshalIndent(setting, "", " ")
	checkError(err, "json.MarshalIndent setting", false, false)

	_, err = file.Write(settingByte)
	checkError(err, "file.Write", false, false)
}

func testConcurrentConsumer(wac *whatsapp.Conn, numAgent int) {

	var wg sync.WaitGroup
	//consumers := make(map[int]func(id int), numAgent)

	log.Println("Getting Alert settings...")
	settings := getAlertSetting()
	writeSetting(settings, globalAlertFile)

	log.Println("Grouping Alert settings...")

	pushAlertSetting(settings)

	for i := 1; i <= numAgent; i++ {
		wg.Add(1)

		go func(wac *whatsapp.Conn, id int) {
			log.Println(fmt.Sprintf("[CONSUMER #%v] Started...", id))
			defer wg.Done()
			for {
				setting := AlertSetting{}

				settingStr := getRedisKey(redisAlertSettingKey)
				if settingStr == "" {
					log.Println("Got EMPTY string from key ", redisAlertSettingKey)
					log.Println(fmt.Sprintf("[CONSUMER #%v] ENDED", id))
					break
				}

				fmt.Println(fmt.Sprintf("[CONSUMER #%v] processAlertSetting: Unmarshall settingStr '%v'", id, settingStr))
				err := json.Unmarshal([]byte(settingStr), &setting)
				//fmt.Println(fmt.Sprintf("processAlertSetting: AFTER Unmarshall settingByte '%v'", []byte(settingStr)))
				checkError(err, "json.Unmarshal", false, false)

				setting.SendAlert(wac, id)
			}
		}(wac, i)
	}

}

func main() {

	redisPool = newRedisPool()

	wac, _, err := waInit()
	//wac, wah, err := waInit()
	checkError(err, "waInit", false, false)

	redisClient = redisPool.Get()
	defer redisClient.Close()

	dbRead = dbConnRead()
	err = dbRead.Ping()
	checkError(err, "dbRead.Ping", false, false)

	dbWrite = dbConnWrite()
	err = dbWrite.Ping()
	checkError(err, "dbWrite.Ping", false, false)

	projects = getProject()

	log.Println("Getting Alert settings...")
	settings := getAlertSetting()
	writeSetting(settings, globalAlertFile)

	log.Println("Grouping Alert settings...")

	pushAlertSetting(settings)

	for i := 1; i <= maxConcurrent; i++ {
		wg.Add(1)

		go func(wac *whatsapp.Conn, id int) {
			log.Println(fmt.Sprintf("[CONSUMER #%v] Started...", id))
			defer wg.Done()
			for {
				setting := AlertSetting{}

				settingStr := getRedisKey(redisAlertSettingKey)
				if settingStr == "" {

					redisClient := redisPool.Get()
					defer redisClient.Close()

					listLength, err := redis.Int(redisClient.Do("LLEN", redisAlertSettingKey))
					checkError(err, "getRedisKey: redisClient.Do LLEN", false, false)
					if listLength > 0 {
						continue
					}

					log.Println("Got EMPTY string from key ", redisAlertSettingKey)
					log.Println(fmt.Sprintf("[CONSUMER #%v] ENDED", id))
					break
				}

				fmt.Println(fmt.Sprintf("[CONSUMER #%v] processAlertSetting: Unmarshall settingStr '%v'", id, settingStr))
				err := json.Unmarshal([]byte(settingStr), &setting)
				//fmt.Println(fmt.Sprintf("processAlertSetting: AFTER Unmarshall settingByte '%v'", []byte(settingStr)))
				checkError(err, "json.Unmarshal", false, false)

				setting.SendAlert(wac, id)
				//wg.Wait()
			}
		}(wac, i)
	}
	wg.Wait()

}
