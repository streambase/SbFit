//
// Copyright (c) 2004-2011 StreamBase Systems, Inc. All rights reserved.
//

package com.streambase.sb.adapter.common.csv;

import com.streambase.sb.StreamBaseException;

public class RFC4180FormatException extends StreamBaseException
{
    private static final long serialVersionUID = 1L;

    public RFC4180FormatException(String a_message)
    {
        super(a_message);
    }

    public RFC4180FormatException(String a_message, Throwable a_cause)
    {
        super(a_message, a_cause);
    }

    public RFC4180FormatException(Throwable a_cause)
    {
        super(a_cause);
    }
}
