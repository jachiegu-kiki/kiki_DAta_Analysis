/* ============================================================
 * core.js — 基础设施层
 * ------------------------------------------------------------
 * 职责：
 *   1. 全局状态变量（RAW、CU、FO 等被多文件共享的数据）
 *   2. AUTH 认证（v2: Gateway Cookie-based SSO）
 *   3. 纯函数工具（数字 / 百分比 / class 名 / hash / 时间格式化）
 *
 * 第一性原理：把"无任何业务逻辑、纯属基础设施"的代码独立出来，
 *            其他文件可以无副作用地引用，便于未来单元测试 / 替换实现。
 * ============================================================ */

var $ = function(id){return document.getElementById(id)};
var API = window.location.origin;
var RAW = null;
var CU = null;
var origExecDate = null;
// 后端返回的级联筛选元数据
var ADL = {};         // advisor_dept_links（兼容）
var ALL_DEPTS = [];   // 兼容旧版
var ALL_ADVS = [];    // 全量顾问列表
var FO = {};          // filter_options: {lines, sub_lines, group_sys, biz_blocks, group_l1, group_advisor}

/* ═══ AUTH (v2: Gateway Cookie-based SSO) ═══
   不再自己做登入；未登入時 Gateway 回 401，跳 /auth/login。
*/
function gotoLogin() { window.location.href = '/auth/login'; }
function doLogout()  { window.location.href = '/auth/logout'; }

async function loadMe() {
  try {
    var res = await fetch(API + '/api/v1/me', { credentials: 'same-origin' });
    if (res.status === 401) { gotoLogin(); return null; }
    if (!res.ok) return null;
    var d = await res.json();
    return {
      role: d.role,                           // finance 层 role
      gw_role: d.gw_role || '',               // v4: gateway 层 role
      is_system_admin: !!d.is_system_admin,   // v4: 后端已算好
      username: d.username,
      dept_scope: d.department_scope,
      advisor_name: d.advisor_name,
      label: (d.display_name || d.username) + ' · ' +
             ({ADMIN:'总盘管理员', MANAGER:'部门经理', ADVISOR:'顾问'}[d.role] || d.role)
    };
  } catch(e) { console.error('loadMe error', e); return null; }
}

/* ═══ UTILS（纯函数 — 渲染层共用）═══ */
function N(v, d) { d = d || 2; return v == null ? '—' : (+v).toFixed(d); }
function P(v) { return v == null ? '—' : (v >= 0 ? '+' : '') + N(v) + '%'; }
function cls(v) { return v == null ? 'nt' : v >= 0 ? 'up' : 'dn'; }
function clsI(v) { return v == null ? 'nt' : v >= 0 ? 'dn' : 'up'; }
function bdg(t, c) { return '<span class="bdg ' + c + '">' + t + '</span>'; }
function hash(s) { var h=0; for(var i=0;i<s.length;i++) h=(h*31+s.charCodeAt(i))&0xffff; return h.toString(16); }

// 日期 → 'YYYY-MM-DD'（本地时区）
function fmtD(d) { return d.getFullYear() + '-' + (d.getMonth()+1 < 10 ? '0' : '') + (d.getMonth()+1) + '-' + (d.getDate() < 10 ? '0' : '') + d.getDate(); }

// UTC ISO → 上海时区人类可读格式（"2026-04-30 18:25:33 (上海)"）
// 奧卡姆剃刀：時區轉換用瀏覽器原生 Intl.DateTimeFormat({timeZone:'Asia/Shanghai'})，零依賴
function fmtShanghai(isoUtc) {
  if (!isoUtc) return null;
  try {
    var d = new Date(isoUtc);
    if (isNaN(d.getTime())) return null;
    var parts = new Intl.DateTimeFormat('zh-CN', {
      timeZone: 'Asia/Shanghai',
      year:  'numeric', month:  '2-digit', day:    '2-digit',
      hour:  '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false
    }).formatToParts(d);
    var lookup = {};
    parts.forEach(function(p) { lookup[p.type] = p.value; });
    return lookup.year + '-' + lookup.month + '-' + lookup.day + ' '
         + lookup.hour + ':' + lookup.minute + ':' + lookup.second + ' (上海)';
  } catch(e) { return null; }
}
