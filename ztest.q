// =======================================================================
// One-Sample Z-Test (alternative = 'less') - Safe Variables
// =======================================================================

// 1. Unbiased Sample Variance and Std Dev
svar: {[x] (count[x] * var x) % -1f + count x};
sdev: {[x] sqrt svar x};

// 2. Calculate the Z-statistic
zstat: {[data; mu0]
    n_val: count data;
    xbar: avg data;
    s_val: sdev data;
    (xbar - mu0) % (s_val % sqrt n_val)
    };

// 3. Normal CDF approximation (Abramowitz & Stegun)
normcdf: {[x]
    sgn: signum x;
    abs_x: abs x;
    b1:  0.319381530;
    b2: -0.356563782;
    b3:  1.781477937;
    b4: -1.821255978;
    b5:  1.330274429;
    p_val: 0.2316419;
    
    t_val: 1f % 1f + p_val * abs_x;
    pdf: (1f % sqrt 2f * acos -1f) * exp -0.5 * abs_x * abs_x;
    cdf: 1f - pdf * t_val * b1 + t_val * (b2 + t_val * (b3 + t_val * (b4 + t_val * b5)));
    
    $[sgn >= 0; cdf; 1f - cdf]
    };

// 4. Main execution function
ztest_1samp_less: {[data; mu0]
    z_val: zstat[data; mu0];
    n_val: count data;
    pval: normcdf z_val;
    
    `z_statistic`n`p_value!(z_val; n_val; pval)
    };

// =======================================================================
// Example Usage
// =======================================================================
data: -1.2 -0.5 -1.5 -2.1 -0.1 -0.8 -0.3 -2.2 -1.1 -0.9;
mu0: 0.0;

result: ztest_1samp_less[data; mu0];
show result;
