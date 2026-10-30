<xml xmlns="http://www.w3.org/1999/xhtml" is_dbot="true" collection="false">
  <variables>
    <variable type="" id="v_ema20" islocal="false" iscloud="false">ema20</variable>
    <variable type="" id="v_bbmid" islocal="false" iscloud="false">bb_mid20</variable>
    <variable type="" id="v_bbup" islocal="false" iscloud="false">bb_up20</variable>
    <variable type="" id="v_stdev" islocal="false" iscloud="false">stdev20</variable>
    <variable type="" id="v_z" islocal="false" iscloud="false">zscore</variable>
    <variable type="" id="v_ema5" islocal="false" iscloud="false">ema5</variable>
    <variable type="" id="v_impulso" islocal="false" iscloud="false">impulso</variable>
    <variable type="" id="v_vol" islocal="false" iscloud="false">volumen_institucional</variable>
    <variable type="" id="v_bbup14" islocal="false" iscloud="false">bb_up14</variable>
    <variable type="" id="v_bbdn14" islocal="false" iscloud="false">bb_dn14</variable>
    <variable type="" id="v_atr" islocal="false" iscloud="false">atr14</variable>
  </variables>
  <block type="trade_definition" id="trade_def_block" x="0" y="0">
    <statement name="TRADE_OPTIONS">
      <block type="trade_definition_market" id="td_market" deletable="false" movable="false">
        <field name="MARKET_LIST">synthetic_index</field>
        <field name="SUBMARKET_LIST">random_index</field>
        <field name="SYMBOL_LIST">1HZ75V</field>
        <next>
          <block type="trade_definition_tradetype" id="td_tradetype" deletable="false" movable="false">
            <field name="TRADETYPECAT_LIST">multiplier</field>
            <field name="TRADETYPE_LIST">multiplier</field>
            <next>
              <block type="trade_definition_contracttype" id="td_contract" deletable="false" movable="false">
                <field name="TYPE_LIST">both</field>
                <next>
                  <block type="trade_definition_candleinterval" id="td_candle" deletable="false" movable="false">
                    <field name="CANDLEINTERVAL_LIST">60</field>
                    <next>
                      <block type="trade_definition_restartbuysell" id="td_restart" deletable="false" movable="false">
                        <field name="TIME_MACHINE_ENABLED">FALSE</field>
                        <next>
                          <block type="trade_definition_restartonerror" id="td_restart_err" deletable="false" movable="false">
                            <field name="RESTARTONERROR">TRUE</field>
                          </block>
                        </next>
                      </block>
                    </next>
                  </block>
                </next>
              </block>
            </next>
          </block>
        </next>
      </block>
    </statement>
    <statement name="SUBMARKET">
      <block type="trade_definition_multiplier" id="td_multiplier">
        <field name="MULTIPLIERTYPE_LIST">100</field>
        <value name="AMOUNT">
          <shadow type="math_number_positive" id="stake_shadow">
            <field name="NUM">0.50</field>
          </shadow>
        </value>
        <statement name="MULTIPLIER_PARAMS">
          <block type="multiplier_take_profit" id="td_tp">
            <value name="AMOUNT">
              <shadow type="math_number_positive" id="tp_shadow">
                <field name="NUM">1.50</field>
              </shadow>
            </value>
            <next>
              <block type="multiplier_stop_loss" id="td_sl">
                <value name="AMOUNT">
                  <shadow type="math_number_positive" id="sl_shadow">
                    <field name="NUM">0.50</field>
                  </shadow>
                </value>
              </block>
            </next>
          </block>
        </statement>
      </block>
    </statement>
  </block>
  <block type="before_purchase" id="before_pur_block" x="0" y="640">
    <statement name="BEFOREPURCHASE_STACK">
      <block type="variables_set" id="set_ema20">
        <field name="VAR" id="v_ema20">ema20</field>
        <value name="VALUE">
          <block type="ema" id="ema20_block">
            <value name="INPUT">
              <block type="ticks" id="ticks_ema20"></block>
            </value>
            <value name="PERIOD">
              <shadow type="math_number" id="p_ema20">
                <field name="NUM">20</field>
              </shadow>
            </value>
          </block>
        </value>
        <next>
          <block type="variables_set" id="set_bbmid">
            <field name="VAR" id="v_bbmid">bb_mid20</field>
            <value name="VALUE">
              <block type="bb" id="bb_mid_block">
                <field name="BBRESULT_LIST">0</field>
                <value name="INPUT">
                  <block type="ticks" id="ticks_bbmid"></block>
                </value>
                <value name="PERIOD">
                  <shadow type="math_number" id="p_bbmid">
                    <field name="NUM">20</field>
                  </shadow>
                </value>
                <value name="UPMULTIPLIER">
                  <shadow type="math_number" id="up_bbmid">
                    <field name="NUM">1</field>
                  </shadow>
                </value>
                <value name="DOWNMULTIPLIER">
                  <shadow type="math_number" id="dn_bbmid">
                    <field name="NUM">1</field>
                  </shadow>
                </value>
              </block>
            </value>
            <next>
              <block type="variables_set" id="set_bbup">
                <field name="VAR" id="v_bbup">bb_up20</field>
                <value name="VALUE">
                  <block type="bb" id="bb_up_block">
                    <field name="BBRESULT_LIST">1</field>
                    <value name="INPUT">
                      <block type="ticks" id="ticks_bbup"></block>
                    </value>
                    <value name="PERIOD">
                      <shadow type="math_number" id="p_bbup">
                        <field name="NUM">20</field>
                      </shadow>
                    </value>
                    <value name="UPMULTIPLIER">
                      <shadow type="math_number" id="up_bbup">
                        <field name="NUM">1</field>
                      </shadow>
                    </value>
                    <value name="DOWNMULTIPLIER">
                      <shadow type="math_number" id="dn_bbup">
                        <field name="NUM">1</field>
                      </shadow>
                    </value>
                  </block>
                </value>
                <next>
                  <block type="variables_set" id="set_stdev">
                    <field name="VAR" id="v_stdev">stdev20</field>
                    <value name="VALUE">
                      <block type="math_arithmetic" id="stdev_math">
                        <field name="OP">MINUS</field>
                        <value name="A">
                          <shadow type="math_number" id="stdev_a_sh">
                            <field name="NUM">0</field>
                          </shadow>
                          <block type="variables_get" id="get_bbup_stdev">
                            <field name="VAR" id="v_bbup">bb_up20</field>
                          </block>
                        </value>
                        <value name="B">
                          <shadow type="math_number" id="stdev_b_sh">
                            <field name="NUM">0</field>
                          </shadow>
                          <block type="variables_get" id="get_bbmid_stdev">
                            <field name="VAR" id="v_bbmid">bb_mid20</field>
                          </block>
                        </value>
                      </block>
                    </value>
                    <next>
                      <block type="variables_set" id="set_z">
                        <field name="VAR" id="v_z">zscore</field>
                        <value name="VALUE">
                          <block type="math_arithmetic" id="z_div">
                            <field name="OP">DIVIDE</field>
                            <value name="A">
                              <shadow type="math_number" id="z_a_sh">
                                <field name="NUM">0</field>
                              </shadow>
                              <block type="math_arithmetic" id="z_num">
                                <field name="OP">MINUS</field>
                                <value name="A">
                                  <shadow type="math_number" id="z_tick_sh">
                                    <field name="NUM">0</field>
                                  </shadow>
                                  <block type="tick" id="tick_z"></block>
                                </value>
                                <value name="B">
                                  <shadow type="math_number" id="z_ema_sh">
                                    <field name="NUM">0</field>
                                  </shadow>
                                  <block type="variables_get" id="get_ema_z">
                                    <field name="VAR" id="v_ema20">ema20</field>
                                  </block>
                                </value>
                              </block>
                            </value>
                            <value name="B">
                              <shadow type="math_number" id="z_den_sh">
                                <field name="NUM">1</field>
                              </shadow>
                              <block type="variables_get" id="get_stdev_z">
                                <field name="VAR" id="v_stdev">stdev20</field>
                              </block>
                            </value>
                          </block>
                        </value>
                        <next>
                          <block type="variables_set" id="set_ema5">
                            <field name="VAR" id="v_ema5">ema5</field>
                            <value name="VALUE">
                              <block type="ema" id="ema5_block">
                                <value name="INPUT">
                                  <block type="ticks" id="ticks_ema5"></block>
                                </value>
                                <value name="PERIOD">
                                  <shadow type="math_number" id="p_ema5">
                                    <field name="NUM">5</field>
                                  </shadow>
                                </value>
                              </block>
                            </value>
                            <next>
                              <block type="variables_set" id="set_impulso">
                                <field name="VAR" id="v_impulso">impulso</field>
                                <value name="VALUE">
                                  <block type="math_arithmetic" id="impulso_abs_proxy">
                                    <field name="OP">MINUS</field>
                                    <value name="A">
                                      <shadow type="math_number" id="imp_a_sh">
                                        <field name="NUM">0</field>
                                      </shadow>
                                      <block type="tick" id="tick_imp"></block>
                                    </value>
                                    <value name="B">
                                      <shadow type="math_number" id="imp_b_sh">
                                        <field name="NUM">0</field>
                                      </shadow>
                                      <block type="variables_get" id="get_ema5_imp">
                                        <field name="VAR" id="v_ema5">ema5</field>
                                      </block>
                                    </value>
                                  </block>
                                </value>
                                <next>
                                  <block type="variables_set" id="set_vol">
                                    <field name="VAR" id="v_vol">volumen_institucional</field>
                                    <value name="VALUE">
                                      <block type="logic_compare" id="vol_cmp">
                                        <field name="OP">GT</field>
                                        <value name="A">
                                          <block type="math_single" id="impulso_abs">
                                            <field name="OP">ABS</field>
                                            <value name="NUM">
                                              <shadow type="math_number" id="abs_sh">
                                                <field name="NUM">0</field>
                                              </shadow>
                                              <block type="variables_get" id="get_impulso">
                                                <field name="VAR" id="v_impulso">impulso</field>
                                              </block>
                                            </value>
                                          </block>
                                        </value>
                                        <value name="B">
                                          <block type="math_arithmetic" id="vol_thr">
                                            <field name="OP">MULTIPLY</field>
                                            <value name="A">
                                              <shadow type="math_number" id="vol_a_sh">
                                                <field name="NUM">2.5</field>
                                              </shadow>
                                            </value>
                                            <value name="B">
                                              <shadow type="math_number" id="vol_b_sh">
                                                <field name="NUM">1</field>
                                              </shadow>
                                              <block type="variables_get" id="get_stdev_vol">
                                                <field name="VAR" id="v_stdev">stdev20</field>
                                              </block>
                                            </value>
                                          </block>
                                        </value>
                                      </block>
                                    </value>
                                    <next>
                                      <block type="variables_set" id="set_bbup14">
                                        <field name="VAR" id="v_bbup14">bb_up14</field>
                                        <value name="VALUE">
                                          <block type="bb" id="bb_up14_block">
                                            <field name="BBRESULT_LIST">1</field>
                                            <value name="INPUT">
                                              <block type="ticks" id="ticks_bbup14"></block>
                                            </value>
                                            <value name="PERIOD">
                                              <shadow type="math_number" id="p_bbup14">
                                                <field name="NUM">14</field>
                                              </shadow>
                                            </value>
                                            <value name="UPMULTIPLIER">
                                              <shadow type="math_number" id="up_bb14">
                                                <field name="NUM">1</field>
                                              </shadow>
                                            </value>
                                            <value name="DOWNMULTIPLIER">
                                              <shadow type="math_number" id="dn_bb14u">
                                                <field name="NUM">1</field>
                                              </shadow>
                                            </value>
                                          </block>
                                        </value>
                                        <next>
                                          <block type="variables_set" id="set_bbdn14">
                                            <field name="VAR" id="v_bbdn14">bb_dn14</field>
                                            <value name="VALUE">
                                              <block type="bb" id="bb_dn14_block">
                                                <field name="BBRESULT_LIST">2</field>
                                                <value name="INPUT">
                                                  <block type="ticks" id="ticks_bbdn14"></block>
                                                </value>
                                                <value name="PERIOD">
                                                  <shadow type="math_number" id="p_bbdn14">
                                                    <field name="NUM">14</field>
                                                  </shadow>
                                                </value>
                                                <value name="UPMULTIPLIER">
                                                  <shadow type="math_number" id="up_bb14d">
                                                    <field name="NUM">1</field>
                                                  </shadow>
                                                </value>
                                                <value name="DOWNMULTIPLIER">
                                                  <shadow type="math_number" id="dn_bb14d">
                                                    <field name="NUM">1</field>
                                                  </shadow>
                                                </value>
                                              </block>
                                            </value>
                                            <next>
                                              <block type="variables_set" id="set_atr">
                                                <field name="VAR" id="v_atr">atr14</field>
                                                <value name="VALUE">
                                                  <block type="math_arithmetic" id="atr_div2">
                                                    <field name="OP">DIVIDE</field>
                                                    <value name="A">
                                                      <shadow type="math_number" id="atr_a_sh">
                                                        <field name="NUM">0</field>
                                                      </shadow>
                                                      <block type="math_arithmetic" id="atr_width">
                                                        <field name="OP">MINUS</field>
                                                        <value name="A">
                                                          <shadow type="math_number" id="atr_up_sh">
                                                            <field name="NUM">0</field>
                                                          </shadow>
                                                          <block type="variables_get" id="get_bbup14">
                                                            <field name="VAR" id="v_bbup14">bb_up14</field>
                                                          </block>
                                                        </value>
                                                        <value name="B">
                                                          <shadow type="math_number" id="atr_dn_sh">
                                                            <field name="NUM">0</field>
                                                          </shadow>
                                                          <block type="variables_get" id="get_bbdn14">
                                                            <field name="VAR" id="v_bbdn14">bb_dn14</field>
                                                          </block>
                                                        </value>
                                                      </block>
                                                    </value>
                                                    <value name="B">
                                                      <shadow type="math_number" id="atr_b_sh">
                                                        <field name="NUM">2</field>
                                                      </shadow>
                                                    </value>
                                                  </block>
                                                </value>
                                                <next>
                                                  <block type="controls_if" id="entry_if">
                                                    <mutation elseif="3"></mutation>
                                                    <value name="IF0">
                                                      <block type="logic_operation" id="mr_up_and">
                                                        <field name="OP">AND</field>
                                                        <value name="A">
                                                          <block type="logic_negate" id="not_vol_up">
                                                            <value name="BOOL">
                                                              <block type="variables_get" id="get_vol_up">
                                                                <field name="VAR" id="v_vol">volumen_institucional</field>
                                                              </block>
                                                            </value>
                                                          </block>
                                                        </value>
                                                        <value name="B">
                                                          <block type="logic_compare" id="z_lt_m2">
                                                            <field name="OP">LT</field>
                                                            <value name="A">
                                                              <block type="variables_get" id="get_z_up">
                                                                <field name="VAR" id="v_z">zscore</field>
                                                              </block>
                                                            </value>
                                                            <value name="B">
                                                              <block type="math_number" id="n_m2">
                                                                <field name="NUM">-2</field>
                                                              </block>
                                                            </value>
                                                          </block>
                                                        </value>
                                                      </block>
                                                    </value>
                                                    <statement name="DO0">
                                                      <block type="purchase" id="buy_mr_up">
                                                        <field name="PURCHASE_LIST">MULTUP</field>
                                                      </block>
                                                    </statement>
                                                    <value name="IF1">
                                                      <block type="logic_operation" id="mr_dn_and">
                                                        <field name="OP">AND</field>
                                                        <value name="A">
                                                          <block type="logic_negate" id="not_vol_dn">
                                                            <value name="BOOL">
                                                              <block type="variables_get" id="get_vol_dn">
                                                                <field name="VAR" id="v_vol">volumen_institucional</field>
                                                              </block>
                                                            </value>
                                                          </block>
                                                        </value>
                                                        <value name="B">
                                                          <block type="logic_compare" id="z_gt_p2">
                                                            <field name="OP">GT</field>
                                                            <value name="A">
                                                              <block type="variables_get" id="get_z_dn">
                                                                <field name="VAR" id="v_z">zscore</field>
                                                              </block>
                                                            </value>
                                                            <value name="B">
                                                              <block type="math_number" id="n_p2">
                                                                <field name="NUM">2</field>
                                                              </block>
                                                            </value>
                                                          </block>
                                                        </value>
                                                      </block>
                                                    </value>
                                                    <statement name="DO1">
                                                      <block type="purchase" id="buy_mr_dn">
                                                        <field name="PURCHASE_LIST">MULTDOWN</field>
                                                      </block>
                                                    </statement>
                                                    <value name="IF2">
                                                      <block type="logic_operation" id="mom_up_and">
                                                        <field name="OP">AND</field>
                                                        <value name="A">
                                                          <block type="variables_get" id="get_vol_mom_up">
                                                            <field name="VAR" id="v_vol">volumen_institucional</field>
                                                          </block>
                                                        </value>
                                                        <value name="B">
                                                          <block type="logic_compare" id="z_gt_p35">
                                                            <field name="OP">GT</field>
                                                            <value name="A">
                                                              <block type="variables_get" id="get_z_mom_up">
                                                                <field name="VAR" id="v_z">zscore</field>
                                                              </block>
                                                            </value>
                                                            <value name="B">
                                                              <block type="math_number" id="n_p35">
                                                                <field name="NUM">3.5</field>
                                                              </block>
                                                            </value>
                                                          </block>
                                                        </value>
                                                      </block>
                                                    </value>
                                                    <statement name="DO2">
                                                      <block type="purchase" id="buy_mom_up">
                                                        <field name="PURCHASE_LIST">MULTUP</field>
                                                      </block>
                                                    </statement>
                                                    <value name="IF3">
                                                      <block type="logic_operation" id="mom_dn_and">
                                                        <field name="OP">AND</field>
                                                        <value name="A">
                                                          <block type="variables_get" id="get_vol_mom_dn">
                                                            <field name="VAR" id="v_vol">volumen_institucional</field>
                                                          </block>
                                                        </value>
                                                        <value name="B">
                                                          <block type="logic_compare" id="z_lt_m35">
                                                            <field name="OP">LT</field>
                                                            <value name="A">
                                                              <block type="variables_get" id="get_z_mom_dn">
                                                                <field name="VAR" id="v_z">zscore</field>
                                                              </block>
                                                            </value>
                                                            <value name="B">
                                                              <block type="math_number" id="n_m35">
                                                                <field name="NUM">-3.5</field>
                                                              </block>
                                                            </value>
                                                          </block>
                                                        </value>
                                                      </block>
                                                    </value>
                                                    <statement name="DO3">
                                                      <block type="purchase" id="buy_mom_dn">
                                                        <field name="PURCHASE_LIST">MULTDOWN</field>
                                                      </block>
                                                    </statement>
                                                  </block>
                                                </next>
                                              </block>
                                            </next>
                                          </block>
                                        </next>
                                      </block>
                                    </next>
                                  </block>
                                </next>
                              </block>
                            </next>
                          </block>
                        </next>
                      </block>
                    </next>
                  </block>
                </next>
              </block>
            </next>
          </block>
        </next>
      </block>
    </statement>
  </block>
  <block type="after_purchase" id="after_pur_block" x="0" y="1400">
    <statement name="AFTERPURCHASE_STACK">
      <block type="trade_again" id="trade_again_block"></block>
    </statement>
  </block>
</xml>
