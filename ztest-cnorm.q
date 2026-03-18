// =======================================================================
// One-Sample Z-Test using pre-defined 'cnorm'
// =======================================================================

// Helper functions for variance and standard deviation
svar: {[x] (count[x] * var x) % -1f + count x};
sdev: {[x] sqrt svar x};

// Main test function
ztest_1samp_less_cnorm: {[data; mu0]
    n_val: count data;
    xbar: avg data;
    s_val: sdev data;
    
    // Calculate Z-statistic
    z_val: (xbar - mu0) % (s_val % sqrt n_val);
    
    // Calculate p-value for alternative='less'
    // For 'less', the p-value is simply the CDF of the z-statistic
    p_val: cnorm z_val;
    
    `z_statistic`n`p_value!(z_val; n_val; p_val)
    };

// =======================================================================
// Example Usage (Assuming cnorm is already loaded)
// =======================================================================
// data: -1.2 -0.5 -1.5 -2.1 -0.1 -0.8 -0.3 -2.2 -1.1 -0.9;
// mu0: 0.0;
// show ztest_1samp_less_cnorm[data; mu0];
