package history

import (
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

var crodObj *cron.Cron

func init() {
	crodObj = cron.New()
	crodObj.Start()
}

func startCrond(job *History) error {
	if job == nil {
		return nil
	}
	job.Lock()
	defer job.Unlock()
	EntryID, err := crodObj.AddJob(job.Property.Crontab, job)
	if err != nil {
		logrus.Printf("[ERROR] history add crontab job DbName:%s SchemaName:%s ID:%d Crontab:%s  err:%+v", job.DbName, job.SchemaName, job.ID, job.Property.Crontab, err)
		return err
	}
	job.cronEntryID = EntryID
	job.cronStatus = HISTORY_STATUS_RUNNING
	return nil
}

func deleteCrond(job *History) error {
	if job == nil {
		return nil
	}
	job.Lock()
	defer job.Unlock()
	if job.cronEntryID != 0 {
		crodObj.Remove(job.cronEntryID)
		job.cronEntryID = 0
		job.cronStatus = HISTORY_STATUS_CLOSE
	}
	return nil
}
