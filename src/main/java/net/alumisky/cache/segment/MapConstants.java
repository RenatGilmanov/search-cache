/*
 * Copyright 2017 AlumiSky (http://alumisky.net). All rights reserved.
 * 
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */
package net.alumisky.cache.segment;

/**
 * This class holds {@see MapSegment} specific constants and configuration.
 * 
 * Separate class is required to replace the constants in order to tune the 
 * {@see MapSegment} implementation for any particular need.
 * 
 * @author Renat.Gilmanov
 */
public class MapConstants {
    public static final int B0 = 4;         // Initial capacity in bits.
    public static final int C0 = 1 << B0;   // Initial capacity (16)
    public static final int B1 = 10;        // Entries array resize limit in bits.
    public static final int C1 = 1 << B1;   // Entries array resize limit (1024).
    public static final int B2 = B1 - B0;   // Sub-maps array length in bits.
    public static final int C2 = 1 << B2;   // Sub-maps array length (64).    
}
