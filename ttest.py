// Load embedPy
\l p.q

// Import scipy.stats
stats: .p.import[`scipy.stats];

// Exact one-sample t-test using Python's scipy engine under the hood
ttest_1samp_less_exact: {[data; mu0]
    // Call scipy.stats.ttest_1samp(data, mu0, alternative='less')
    // Note: pykw is used to pass keyword arguments in embedPy
    res: stats[`:ttest_1samp][data; mu0; `alternative pykw `less];
    
    // Extract the results back into q floats
    t_stat: res[`:statistic][`];
    p_val: res[`:pvalue][`];
    df: -1f + count data;
    
    // Return a q dictionary
    `t_statistic`df`p_value!(t_stat; df; p_val)
    };

// =======================================================================
// Example Usage
// =======================================================================
data: -1.2 -0.5 -1.5 -2.1 -0.1 -0.8 -0.3 -2.2 -1.1 -0.9;
mu0: 0.0;

result: ttest_1samp_less_exact[data; mu0];
show result;
