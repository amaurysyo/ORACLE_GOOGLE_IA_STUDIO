import React from "react";

type LeadLag = {
  best_lag_s?: number | null;
  best_abs_pearson?: number | null;
  dir?: string;
};

type Correlations = {
  pearson?: number | null;
};

type EventImpact = {
  kinds?: Record<
    string,
    {
      mean_return?: Record<string, number | null>;
      hit_rate?: Record<string, number | null>;
    }
  >;
};

type AdvancedResults = {
  leadlag?: LeadLag;
  correlations?: Correlations;
  event_impact?: EventImpact;
};

type ExperimentRun = {
  run_id: string;
  results_version?: string;
  results_json?: {
    advanced?: AdvancedResults;
  };
};

type ExperimentsPageProps = {
  runs: ExperimentRun[];
};

const formatNumber = (value?: number | null) =>
  value === null || value === undefined ? "-" : value.toFixed(3);

const Experiments: React.FC<ExperimentsPageProps> = ({ runs }) => {
  return (
    <div>
      <h1>Experiments</h1>
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>Best Lag (s)</th>
            <th>Abs Corr</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => {
            const leadlag = run.results_json?.advanced?.leadlag;
            return (
              <tr key={run.run_id}>
                <td>{run.run_id}</td>
                <td>{leadlag?.best_lag_s ?? "-"}</td>
                <td>{formatNumber(leadlag?.best_abs_pearson ?? null)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>

      <section>
        <h2>Advanced</h2>
        {runs.map((run) => {
          const advanced = run.results_json?.advanced;
          const leadlag = advanced?.leadlag;
          const pearson = advanced?.correlations?.pearson;
          const kinds = advanced?.event_impact?.kinds || {};
          const topKind = Object.keys(kinds)[0];
          const topKindData = topKind ? kinds[topKind] : undefined;
          const meanReturn1h = topKindData?.mean_return?.["3600"];
          const hitRate1h = topKindData?.hit_rate?.["3600"];
          return (
            <div key={`${run.run_id}-advanced`}>
              <h3>{run.run_id}</h3>
              <div>Lead/Lag: {leadlag?.best_lag_s ?? "-"}</div>
              <div>Best Corr: {formatNumber(leadlag?.best_abs_pearson ?? null)}</div>
              <div>Direction: {leadlag?.dir ?? "-"}</div>
              <div>Correlation: {formatNumber(pearson ?? null)}</div>
              <div>
                Event Impact (1h):
                {topKind
                  ? ` ${topKind} mean=${formatNumber(meanReturn1h ?? null)} hit=${formatNumber(
                      hitRate1h ?? null,
                    )}`
                  : " -"}
              </div>
            </div>
          );
        })}
      </section>
    </div>
  );
};

export default Experiments;
