// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/util/types"
)

// SubQuery is an exprNode with a plan.
type subquery struct {
	ast.ExprNode
	types.DataItem
	flag uint64
	plan plan.Plan
	is   infoschema.InfoSchema
}

// SetValue implements Expression interface.
func (sq *subquery) SetValue(val interface{}) {
	sq.Data = val
}

// GetValue implements Expression interface.
func (sq *subquery) GetValue() interface{} {
	return sq.Data
}
func (sq *subquery) Accept(v ast.Visitor) (ast.Node, bool) {
	// SubQuery is not a normal ExprNode
	// Do nothing
	newNode, skipChildren := v.Enter(sq)
	if skipChildren {
		return v.Leave(newNode)
	}
	sq = newNode.(*subquery)
	return v.Leave(sq)
}

func (sq *subquery) UseOuterQuery() bool {
	return true
}

func (sq *subquery) EvalRows(ctx context.Context, rowCount int) ([]interface{}, error) {
	fmt.Println("Run Eval")
	b := newExecutorBuilder(ctx, sq.is)
	plan.Refine(sq.plan)
	e := b.build(sq.plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}
	if len(e.Fields()) == 0 {
		// No result fields means no Recordset.
		defer e.Close()
		for {
			row, err := e.Next()
			if err != nil || row == nil {
				return nil, errors.Trace(err)
			}
		}
	}
	var (
		err  error
		row  *Row
		rows = []interface{}{}
	)
	for rowCount != 0 {
		row, err = e.Next()
		if err != nil {
			return rows, errors.Trace(err)
		}
		if row == nil {
			break
		}
		if len(row.Data) == 1 {
			rows = append(rows, row.Data[0])
		} else {
			rows = append(rows, row.Data)
		}
		if rowCount > 0 {
			rowCount--
		}
	}
	return rows, nil
}

func (sq *subquery) ColumnCount() (int, error) {
	fmt.Println("Call ColumnCount!", len(sq.plan.Fields()))
	return len(sq.plan.Fields()), nil
}

type subqueryBuilder struct {
	is infoschema.InfoSchema
}

func (sb *subqueryBuilder) Build(p plan.Plan) ast.SubQuery {
	return &subquery{
		is:   sb.is,
		plan: p,
	}
}
