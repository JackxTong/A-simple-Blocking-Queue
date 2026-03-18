// =======================================================================
// One-Sample t-test (alternative = 'less')
// =======================================================================

// 1. Calculate Unbiased Sample Variance (dividing by n-1 instead of n)
svar: {[x] (count[x] * var x) % -1f + count x};

// 2. Calculate Sample Standard Deviation
sdev: {[x] sqrt svar x};

// 3. Calculate the exact t-statistic
// data: float list, mu0: null hypothesis mean
tstat: {[data; mu0]
    n: count data;
    xbar: avg data;
    s: sdev data;
    (xbar - mu0) % (s % sqrt n)
    };

// 4. Normal CDF approximation (Abramowitz & Stegun)
// Used to approximate the t-distribution p-value
normcdf: {[x]
    sign: signum x;
    x: abs x;
    b1:  0.319381530;
    b2: -0.356563782;
    b3:  1.781477937;
    b4: -1.821255978;
    b5:  1.330274429;
    p:   0.2316419;
    t: 1f % 1f + p * x;
    pdf: (1f % sqrt 2f * acos -1f) * exp -0.5 * x * x;
    cdf: 1f - pdf * t * b1 + t * (b2 + t * (b3 + t * (b4 + t * b5)));
    $[sign >= 0; cdf; 1f - cdf]
    };

// 5. Approximate t-distribution CDF
// t-distribution approaches normal distribution, slightly adjusted for df
tcdf_approx: {[t; df]
    z: t * 1f - 1f % 4f * df;
    normcdf z
    };

// 6. Main execution function
// Computes t-stat, degrees of freedom, and p-value for alternative='less'
ttest_1samp_less: {[data; mu0]
    t: tstat[data; mu0];
    df: -1f + count data;
    pval: tcdf_approx[t; df];
    
    // Return a dictionary with the results
    `t_statistic`df`p_value!(t; df; pval)
    };

// =======================================================================
// Example Usage
// =======================================================================

// Dummy dataset
data: -1.2 -0.5 -1.5 -2.1 -0.1 -0.8 -0.3 -2.2 -1.1 -0.9;
mu0: 0.0;

result: ttest_1samp_less[data; mu0];

show result;
