// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * like expression: a like 'xxx%'.
 */
public class Like<LEFT_CHILD_TYPE extends Expression, RIGHT_CHILD_TYPE extends Expression>
        extends StringRegexPredicate<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    public Like(LEFT_CHILD_TYPE left, RIGHT_CHILD_TYPE right) {
        super(NodeType.LIKE, left, right);
    }

    @Override
    public boolean nullable() throws UnboundException {
        return left().nullable();
    }

    @Override
    public String toString() {
        return "(" + left() + " like " + right() + ")";
    }

    @Override
    public Like<Expression, Expression> withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new Like<>(children.get(0), children.get(1));
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLike(this, context);
    }
}
