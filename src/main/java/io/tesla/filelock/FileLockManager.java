package io.tesla.filelock;

/*******************************************************************************
 * Copyright (c) 2010-2013 Sonatype, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

import java.io.File;

/**
 * A LockManager holding external locks, locking files between OS processes (e.g. via {@link Lock}.
 * 
 * @author Benjamin Hanzelmann
 */
public interface FileLockManager {

  /**
   * Obtain a lock object that may be used to lock the target file for reading. This method must not lock that file
   * right immediately (see {@link Lock#lock()}).
   * 
   * @param target the file to lock, never {@code null}.
   * @return a lock object, never {@code null}.
   */
  Lock readLock(File target);

  /**
   * Obtain a lock object that may be used to lock the target file for writing. This method must not lock that file
   * right immediately (see {@link Lock#lock()}).
   * 
   * @param target the file to lock, never {@code null}.
   * @return a lock object, never {@code null}.
   */
  Lock writeLock(File target);
}
