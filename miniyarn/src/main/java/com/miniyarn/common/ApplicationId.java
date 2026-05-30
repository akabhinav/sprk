package com.miniyarn.common;

import java.io.Serializable;

/** An application's identity in the RM. Real YARN: ApplicationId. */
public record ApplicationId(int id) implements Serializable {
    @Override public String toString() { return "app_" + id; }
}
