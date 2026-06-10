{#
  Shared rate-stat expression generators for the play-by-play marts. Each macro
  assumes a set of summed component columns is in scope (see column names below)
  plus a `weights` row alias (default `w`) joined by season for wOBA/FIP constants.
  Used by every season/monthly/splits mart AND ad-hoc custom-period queries, so
  the formulas live in exactly one place.
#}

{% macro pbp_batting_rate_stats(w='w') %}
    {#- expects: pa, ab, h, singles, doubles, triples, hr, ubb, bb, ibb, hbp, so, sf -#}
    round(case when ab > 0 then h::numeric / ab end, 3) as avg,
    round(case when (ab + bb + hbp + sf) > 0 then (h + bb + hbp)::numeric / (ab + bb + hbp + sf) end, 3) as obp,
    round(case when ab > 0 then (singles + 2 * doubles + 3 * triples + 4 * hr)::numeric / ab end, 3) as slg,
    round(case
        when ab > 0 then ((singles + 2 * doubles + 3 * triples + 4 * hr)::numeric / ab)
            + (case when (ab + bb + hbp + sf) > 0 then (h + bb + hbp)::numeric / (ab + bb + hbp + sf) else 0 end)
    end, 3) as ops,
    round(case when ab > 0 then (doubles + 2 * triples + 3 * hr)::numeric / ab end, 3) as iso,
    round(case when (ab - so - hr + sf) > 0 then (h - hr)::numeric / (ab - so - hr + sf) end, 3) as babip,
    round(case when pa > 0 then bb::numeric / pa end, 3) as bb_pct,
    round(case when pa > 0 then so::numeric / pa end, 3) as k_pct,
    round(case
        when (ab + bb - ibb + sf + hbp) > 0 then (
            {{ w }}.w_bb * ubb + {{ w }}.w_hbp * hbp + {{ w }}.w_single * singles
            + {{ w }}.w_double * doubles + {{ w }}.w_triple * triples + {{ w }}.w_home_run * hr
        )::numeric / (ab + bb - ibb + sf + hbp)
    end, 3) as woba
{% endmacro %}


{% macro pbp_pitching_rate_stats(w='w') %}
    {#- expects: bf, ab, outs, h, bb, ubb, ibb, hbp, hr, so, r, er -#}
    round((outs / 3.0)::numeric, 1) as ip,
    round(case when ab > 0 then h::numeric / ab end, 3) as baa,
    round(case when outs > 0 then 9.0 * er / (outs / 3.0) end, 2) as era,
    round(case when outs > 0 then (bb + h)::numeric / (outs / 3.0) end, 3) as whip,
    round(case when outs > 0 then 9.0 * so / (outs / 3.0) end, 2) as k9,
    round(case when outs > 0 then 9.0 * bb / (outs / 3.0) end, 2) as bb9,
    round(case when outs > 0 then 9.0 * hr / (outs / 3.0) end, 2) as hr9,
    round(case when bf > 0 then so::numeric / bf end, 3) as k_pct,
    round(case when bf > 0 then bb::numeric / bf end, 3) as bb_pct,
    round(case
        when outs > 0
        then (((13 * hr + 3 * (ubb + hbp) - 2 * so)::numeric / (outs / 3.0)) + {{ w }}.c_fip)::numeric
    end, 2) as fip
{% endmacro %}
