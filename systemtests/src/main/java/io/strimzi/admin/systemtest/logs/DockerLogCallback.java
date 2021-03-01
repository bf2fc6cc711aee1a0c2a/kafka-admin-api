/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.logs;

import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;


public class DockerLogCallback extends ResultCallback.Adapter<Frame> {
    protected final StringBuffer log = new StringBuffer();

    public DockerLogCallback() {
    }

    @Override
    public void onNext(Frame frame) {
        log.append(new String(frame.getPayload()));
    }

    @Override
    public String toString() {
        return log.toString();
    }
}
