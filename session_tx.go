// Copyright 2016 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"time"
)

// Begin a transaction
func (session *Session) Begin() error {
	if session.isAutoCommit {
		tx, err := session.DB().Begin()
		if err != nil {
			return err
		}
		session.isAutoCommit = false
		session.isCommitedOrRollbacked = false
		session.tx = tx
		session.saveLastSQL("BEGIN TRANSACTION")

		{
			ti := &tracingInfo{}
			ti.startTime = time.Now()
			ti.operation = SessionOpTransaction
			ti.lastSQL = session.lastSQL
			ti.lastSQLArgs = session.lastSQLArgs
			ti.dbType = string(session.engine.dialect.DBType())
			ti.isTx = true
			session.prepareTracingSpan(ti)
		}
	}
	return nil
}

// Rollback When using transaction, you can rollback if any error
func (session *Session) Rollback() error {
	if !session.isAutoCommit && !session.isCommitedOrRollbacked {
		session.saveLastSQL(session.engine.dialect.RollBackStr())
		defer func() {
			session.tracingInfo.logEvent("ROLLBACK")
		}()

		session.isCommitedOrRollbacked = true
		session.isAutoCommit = true
		return session.tx.Rollback()
	}
	return nil
}

// Commit When using transaction, Commit will commit all operations.
func (session *Session) Commit() error {
	if !session.isAutoCommit && !session.isCommitedOrRollbacked {
		session.saveLastSQL("COMMIT")
		defer func() {
			if session.tracingInfo != nil {
				session.tracingInfo.txCommit = true
				session.tracingInfo.logEvent("COMMIT")
			}
		}()

		session.isCommitedOrRollbacked = true
		session.isAutoCommit = true
		var err error
		if err = session.tx.Commit(); err == nil {
			// handle processors after tx committed
			closureCallFunc := func(closuresPtr *[]func(interface{}), bean interface{}) {
				if closuresPtr != nil {
					for _, closure := range *closuresPtr {
						closure(bean)
					}
				}
			}

			for bean, closuresPtr := range session.afterInsertBeans {
				closureCallFunc(closuresPtr, bean)

				if processor, ok := interface{}(bean).(AfterInsertProcessor); ok {
					processor.AfterInsert()
				}
			}
			for bean, closuresPtr := range session.afterUpdateBeans {
				closureCallFunc(closuresPtr, bean)

				if processor, ok := interface{}(bean).(AfterUpdateProcessor); ok {
					processor.AfterUpdate()
				}
			}
			for bean, closuresPtr := range session.afterDeleteBeans {
				closureCallFunc(closuresPtr, bean)

				if processor, ok := interface{}(bean).(AfterDeleteProcessor); ok {
					processor.AfterDelete()
				}
			}
			cleanUpFunc := func(slices *map[interface{}]*[]func(interface{})) {
				if len(*slices) > 0 {
					*slices = make(map[interface{}]*[]func(interface{}), 0)
				}
			}
			cleanUpFunc(&session.afterInsertBeans)
			cleanUpFunc(&session.afterUpdateBeans)
			cleanUpFunc(&session.afterDeleteBeans)
		}
		return err
	}
	return nil
}
