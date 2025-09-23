// Unity Auto LeadBot 2.0.1 — Node 16, CommonJS
const {
  MSG_CALC_INTRO,
  MSG_QA_INTRO,
  MSG_ASK_MAKE_MODEL,
  MSG_ASK_CONDITION,
  MSG_CUSTOMS_NOTE,
  MSG_THANKS_SERVICES,
  mdv2, // helper для экранирования Markdown-V2
} = require('./messages');

require('dotenv').config();
const { Telegraf, Markup } = require('telegraf');
const fetch = require('node-fetch'); // v2 для Node 16
const { ulid } = require('ulid');
const dayjs = require('dayjs');
const fs = require('fs');
const path = require('path');
const pino = require('pino');
const crypto = require('crypto');

// ----------------- Логгер -----------------
const LOG = pino({ level: process.env.LOG_LEVEL || 'info' });

// ----------------- Пути и файлы -----------------
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
const OUTBOX_PATH = path.join(DATA_DIR, 'outbox.json');
const DEDUP_PATH  = path.join(DATA_DIR, 'dedup.json');
const RR_PATH     = path.join(DATA_DIR, 'rr.json');
const FAQ_PATH    = path.join(__dirname, 'faq.json');
const OAUTH_PATH  = path.join(DATA_DIR, 'amo_oauth.json');

// ----------------- Конфиг окружения -----------------
const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_CHAT_ID = Number(process.env.ADMIN_CHAT_ID || 0);
const VIN_BOT_URL = process.env.VIN_BOT_URL || 'https://t.me';
const NEWS_CHANNEL_URL = process.env.NEWS_CHANNEL_URL || 'https://t.me';
const CATALOG_URL = process.env.CATALOG_URL || 'https://example.com';

const MANAGER_TG_URL = process.env.MANAGER_TG_URL || 'https://t.me/your_manager_username';

// amoCRM OAuth2/база
const AMO_BASE_URL = process.env.AMO_BASE_URL;
const AMO_CLIENT_ID = process.env.AMO_CLIENT_ID;
const AMO_CLIENT_SECRET = process.env.AMO_CLIENT_SECRET;
const AMO_REDIRECT_URI = process.env.AMO_REDIRECT_URI;
const AMO_REFRESH_TOKEN = process.env.AMO_REFRESH_TOKEN;

// Пайплайн/статус
const AMO_PIPELINE_IMPORT_ID = Number(process.env.AMO_PIPELINE_IMPORT_ID || 0);
const AMO_STATUS_IMPORT_NEW_ID = Number(process.env.AMO_STATUS_IMPORT_NEW_ID || 0);

// Кастомные поля LEAD
const CF = {
  MODEL:          Number(process.env.AMO_CF_LEAD_MODEL_ID || 0),
  CONDITION:      Number(process.env.AMO_CF_LEAD_CONDITION_ID || 0),
  YEAR:           Number(process.env.AMO_CF_LEAD_YEAR_ID || 0),
  MILEAGE_MAX:    Number(process.env.AMO_CF_LEAD_MILEAGE_MAX_ID || 0),
  DELIVERY_CITY:  Number(process.env.AMO_CF_LEAD_DELIVERY_CITY_ID || 0),
  COUNTRY_TITLE:  Number(process.env.AMO_CF_LEAD_COUNTRY_TITLE_ID || 0),
  COMMENT_TEXT:   Number(process.env.AMO_CF_LEAD_COMMENT_TEXT_ID || 0),
  CONTACT_METHOD: Number(process.env.AMO_CF_LEAD_CONTACT_METHOD_ID || 0),
  CONTACT_PHONE:  Number(process.env.AMO_CF_LEAD_CONTACT_PHONE_ID || 0),
  CONTACT_TG:     Number(process.env.AMO_CF_LEAD_CONTACT_TG_ID || 0), // опционально
  CITY:           Number(process.env.AMO_CF_LEAD_CITY_ID || 0),       // если есть
  COUNTRY:        Number(process.env.AMO_CF_LEAD_COUNTRY_ID || 0),    // если есть кодовое
};

// Кастомное поле CONTACT (Telegram) — опционально
const AMO_CF_CONTACT_TELEGRAM_ID = Number(process.env.AMO_CF_CONTACT_TELEGRAM_ID || 0);

// Роутинг
const RESPONSIBLES = String(process.env.AMO_RESPONSIBLES || '')
  .split(',')
  .map(s => Number(s.trim()))
  .filter(Boolean);

// ----------------- Состояния/хранилища -----------------
const S = new Map();
const outbox = loadJSON(OUTBOX_PATH, []);
// структура dedup: { queued: {key: ts}, sent: {key: ts} }
const dedup  = loadJSON(DEDUP_PATH, { queued: {}, sent: {} });
// миграция старого формата (мог быть пустой объект {})
if (!dedup || typeof dedup !== 'object') {
  // в крайне редком случае повреждения файла
  Object.assign((dedup || {}), { queued: {}, sent: {} });
}
if (!dedup.queued || typeof dedup.queued !== 'object') dedup.queued = {};
if (!dedup.sent   || typeof dedup.sent   !== 'object') dedup.sent   = {};
try { saveJSON(DEDUP_PATH, dedup); } catch {}
const rr     = loadJSON(RR_PATH, { i: 0 });

// ----------------- FAQ -----------------
const FAQ_PAGE_SIZE = 8;
const FAQ = loadJSON(FAQ_PATH, { sections: [] });
const FAQIndex = buildFaqIndex(FAQ);

function buildFaqIndex(faq) {
  const secById = new Map();
  const subByKey = new Map();
  const qById = new Map();
  for (const sec of faq.sections || []) {
    secById.set(sec.id, sec);
    for (const sub of sec.subs || []) {
      subByKey.set(`${sec.id}:${sub.id}`, sub);
      for (const q of sub.qs || []) {
        qById.set(q.id, { ...q, secId: sec.id, subId: sub.id, secTitle: sec.title, subTitle: sub.title });
      }
    }
  }
  return { secById, subByKey, qById };
}

// ----------------- Утилиты -----------------
function loadJSON(file, fallback) {
  try {
    if (fs.existsSync(file)) {
      return JSON.parse(fs.readFileSync(file, 'utf8'));
    }
    // если файла нет — создаём с дефолтом
    fs.writeFileSync(file, JSON.stringify(fallback, null, 2));
    return fallback;
  } catch (e) {
    LOG.warn({ err: String(e) }, `loadJSON error: ${file}`);
    return fallback;
  }
}
function saveJSON(file, data) { fs.writeFileSync(file, JSON.stringify(data, null, 2)); }
function nowISO() { return new Date().toISOString(); }

// ====== Idempotency / Dedup ======
function sha1(s){ return crypto.createHash('sha1').update(String(s)).digest('hex'); }
function dedupKeyFromPayload(p) {
  // приоритет: lead_uid → flow_uid → хеш содержимого
  if (p.lead_uid) return `lead:${p.lead_uid}`;
  if (p.flow_uid) return `flow:${p.flow_uid}`;
  const basis = JSON.stringify({
    bm: p.bm, city: p.city, country: p.country, condition: p.condition,
    used_year: p.used_year, used_mileage: p.used_mileage,
    phone: p?.contact?.phone_e164 || null, tg: p?.contact?.tg_username || null
  });
  return `hash:${sha1(basis)}`;
}
function dedupSeen(key, ttlMs = 24*60*60*1000) {
  const sent = (dedup.sent ||= {});
  const t = sent[key] || 0;
  return t && (Date.now() - t) < ttlMs;
}
function dedupQueued(key, ttlMs = 10*60*1000) {
  const queued = (dedup.queued ||= {});
  const t = queued[key] || 0;
  return t && (Date.now() - t) < ttlMs;
}
function markQueued(key) {
  (dedup.queued ||= {});
  dedup.queued[key] = Date.now();
  saveJSON(DEDUP_PATH, dedup);
}
function markSent(key) {
  (dedup.sent ||= {});
  (dedup.queued ||= {});
  dedup.sent[key] = Date.now();
  delete dedup.queued[key];
  saveJSON(DEDUP_PATH, dedup);
}
function dedupCleanup() {
  const now = Date.now();
  const queued = (dedup.queued ||= {});
  const sent   = (dedup.sent   ||= {});
  for (const [k, t] of Object.entries(queued)) if (now - t > 24*60*60*1000) delete queued[k];
  for (const [k, t] of Object.entries(sent))   if (now - t > 7*24*60*1000*24) delete sent[k];
  saveJSON(DEDUP_PATH, dedup);
}

// ====== Текст → HTML для Telegram ======
function stripMdV2Escapes(s) {
  return String(s || '').replace(/\\([_*\[\]()~`>#+\-=|{}.!\\])/g, '$1');
}
function toHtmlForTelegram(s) {
  let t = stripMdV2Escapes(s);
  t = t.replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g, '<a href="$2">$1</a>');
  t = t.replace(/(^|\n)\s*-\s+/g, '$1• ');
  return t; // \n остаются как переносы
}

function parseUTM(startParam) {
  const res = { source: '', medium: '', campaign: '', content: '' };
  if (!startParam) return res;
  const parts = startParam.split('~');
  for (const p of parts) {
    const [k, v] = p.split(':');
    if (k === 'utm_source') res.source = v;
    if (k === 'utm_medium') res.medium = v;
    if (k === 'utm_campaign') res.campaign = v;
    if (k === 'utm_content') res.content = v;
  }
  return res;
}
function capitalizeWords(s) {
  return String(s || '').trim().replace(/\p{L}+/gu, w => w[0].toUpperCase() + w.slice(1).toLowerCase());
}

// --- Валидации ---
function validateBrandModel(t) {
  const s = String(t || '').trim();
  if (s.length < 2 || s.length > 40) return false;
  return /^[A-Za-z0-9][A-Za-z0-9\s\-]+$/.test(s);
}
function cleanCityInput(t) {
  return String(t || '')
    .trim()
    .replace(/^(г\.?|город)\s+/i, '')
    .replace(/\s{2,}/g, ' ');
}
function isCityValid(t) {
  return /^[\p{L}\s\-’']+$/u.test(t) && t.length >= 2 && t.length <= 50;
}
function normalizeCityOrNull(raw) {
  let t = cleanCityInput(raw);
  if (!isCityValid(t)) return null;
  t = t.replace(/[-–—]/g, '-');
  return capitalizeWords(t);
}
function parseYear(t) {
  const y = Number(String(t).trim().match(/\b(19[6-9]\d|20[0-2]\d|2025)\b/)?.[0] || NaN);
  if (!Number.isInteger(y) || y < 1960 || y > 2025) return null;
  return y;
}
function parseMileage(t) {
  let s = String(t || '').toLowerCase();
  const km = Number(s.replace(/[^\d]/g, ''));
  if (!Number.isFinite(km) || km <= 0) return null;
  return km; // «до N км»
}
function fmt(n) { return (n || 0).toLocaleString('ru-RU'); }

// ----------------- Константы UI и прайсы -----------------
const COUNTRIES = [
  { code: 'EU', flag: '🇪🇺', title: 'Европа' },
  { code: 'KR', flag: '🇰🇷', title: 'Корея' },
  { code: 'CN', flag: '🇨🇳', title: 'Китай' },
  { code: 'US', flag: '🇺🇸', title: 'США' },
  { code: 'AE', flag: '🇦🇪', title: 'ОАЭ' }
];

const COST = {
  CN: { delivery: 150000, days: 20, service: 150000 },
  EU: { delivery: 450000, days: 45, service: 150000 },
  AE: { delivery: 290000, days: 45, service: 150000 },
  US: { delivery: 350000, days: 60, service: 150000 },
  KR: { delivery: 250000, days: 30, service: 150000 }
};

const PRESET_CITIES = [
  { k: 'msk', t: 'Москва' },
  { k: 'spb', t: 'Санкт-Петербург' },
  { k: 'oth', t: 'Указать свой' }
];

// ----------------- AmoCRM клиент -----------------
const amo = (() => {
  // refresh_token ротируется — сохраняем его на диск
  const oauthStore = loadJSON(OAUTH_PATH, { refresh_token: process.env.AMO_REFRESH_TOKEN || '' });
  let refreshingPromise = null;

  async function refreshToken(force = false) {
    if (refreshingPromise && !force) return refreshingPromise;
    refreshingPromise = (async () => {
      const refresh_token = oauthStore.refresh_token || process.env.AMO_REFRESH_TOKEN || '';
      if (!refresh_token) throw new Error('AMO refresh_token is missing');
      const url = `${AMO_BASE_URL}/oauth2/access_token`;
      const body = {
        client_id: AMO_CLIENT_ID,
        client_secret: AMO_CLIENT_SECRET,
        grant_type: 'refresh_token',
        refresh_token,
        redirect_uri: AMO_REDIRECT_URI
      };
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        body: JSON.stringify(body)
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`AMO refresh failed: ${res.status} ${txt}`);
      }
      const data = await res.json();
      amo.accessToken = data.access_token;
      amo.expiresAt = Date.now() + Math.max(0, (data.expires_in || 0) - 60) * 1000;
      if (data.refresh_token && data.refresh_token !== oauthStore.refresh_token) {
        oauthStore.refresh_token = data.refresh_token;
        saveJSON(OAUTH_PATH, oauthStore);
        LOG.info('amoCRM refresh_token rotated and saved');
      }
      return true;
    })().finally(() => { refreshingPromise = null; });
    return refreshingPromise;
  }
  async function ensureToken() {
    if (!amo.accessToken || !amo.expiresAt || Date.now() > amo.expiresAt) {
      await refreshToken();
    }
  }
  async function api(method, path, body) {
    await ensureToken();
    const doFetch = async () => fetch(`${AMO_BASE_URL}${path}`, {
      method,
      headers: {
        'Authorization': `Bearer ${amo.accessToken}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: body ? JSON.stringify(body) : undefined
    });
    let res = await doFetch();
    if (res.status === 401) {
      LOG.warn({ path }, '401 from amoCRM, refreshing token and retrying once');
      await refreshToken(true);
      res = await doFetch();
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      throw new Error(`AMO ${method} ${path} -> ${res.status}: ${txt}`);
    }
    if (res.status === 204) return null;
    return res.json();
  }
  function cfValue(field_id, value) { return { field_id, values: [{ value }] }; }

  // ---- распознаём "плохой" кастом-филд ----
  function isBadCFError(e) {
    const s = String(e || '');
    return s.includes('NotSupportedChoice') && s.includes('custom_fields_values') && s.includes('field_id');
  }

  async function createOrUpdateContact({ phone_e164, name, tg_username }) {
    // --- PATCH существующего по телефону ---
    if (phone_e164) {
      const q = encodeURIComponent(phone_e164);
      const data = await api('GET', `/api/v4/contacts?limit=1&query=${q}`);
      const found = data?._embedded?.contacts?.[0];

      const contactCF = [];
      if (AMO_CF_CONTACT_TELEGRAM_ID && tg_username) {
        contactCF.push(cfValue(AMO_CF_CONTACT_TELEGRAM_ID, `@${tg_username}`));
      }

      if (found) {
        const basePatch = { id: found.id, name: name || found.name || phone_e164 };
        try {
          const body = [ contactCF.length ? { ...basePatch, custom_fields_values: contactCF } : basePatch ];
          await api('PATCH', `/api/v4/contacts`, body);
        } catch (e) {
          if (isBadCFError(e) && contactCF.length) {
            LOG.warn({ err: String(e) }, 'contact PATCH failed on custom field, retrying without it');
            await api('PATCH', `/api/v4/contacts`, [ basePatch ]);
          } else {
            throw e;
          }
        }
        if (tg_username) {
          try { await addContactNote(found.id, `Telegram: @${tg_username}`); } catch {}
        }
        return found.id;
      }
    }

    // --- POST (новый контакт) ---
    const bodyBase = {
      name: name || (tg_username ? `@${tg_username}` : (phone_e164 || 'Telegram lead')),
      custom_fields_values: []
    };
    if (phone_e164) bodyBase.custom_fields_values.push({ field_code: 'PHONE', values: [{ value: phone_e164 }] });
    if (AMO_CF_CONTACT_TELEGRAM_ID && tg_username) {
      bodyBase.custom_fields_values.push(cfValue(AMO_CF_CONTACT_TELEGRAM_ID, `@${tg_username}`));
    }

    try {
      const resp = await api('POST', `/api/v4/contacts`, [bodyBase]);
      const id = resp._embedded.contacts[0].id;
      if (tg_username) { try { await addContactNote(id, `Telegram: @${tg_username}`); } catch {} }
      return id;
    } catch (e) {
      if (isBadCFError(e) && AMO_CF_CONTACT_TELEGRAM_ID && tg_username) {
        LOG.warn({ err: String(e) }, 'contact POST failed on custom field, retrying without it');
        const safeBody = { ...bodyBase, custom_fields_values: bodyBase.custom_fields_values
          .filter(v => !('field_id' in v)) };
        const resp2 = await api('POST', `/api/v4/contacts`, [safeBody]);
        const id2 = resp2._embedded.contacts[0].id;
        if (tg_username) { try { await addContactNote(id2, `Telegram: @${tg_username}`); } catch {} }
        return id2;
      }
      throw e;
    }
  }

  async function addContactNote(contact_id, text) {
    const body = [{ note_type: 'common', params: { text } }];
    await api('POST', `/api/v4/contacts/${contact_id}/notes`, body);
  }

  async function createLeadCalc({ payload, responsible_id }) {
    const { bm, condition, used_year, used_mileage, city, country, comment, contact } = payload;
    const pipeline_id = AMO_PIPELINE_IMPORT_ID;
    const status_id = AMO_STATUS_IMPORT_NEW_ID;

    const cfs = [];
    if (CF.MODEL && bm) cfs.push(cfValue(CF.MODEL, bm));
    if (CF.CONDITION && condition) cfs.push(cfValue(CF.CONDITION, condition === 'new' ? 'Новый' : 'С пробегом'));
    if (CF.YEAR && used_year) cfs.push({ field_id: CF.YEAR, values: [{ value: used_year }] });
    if (CF.MILEAGE_MAX && used_mileage) cfs.push({ field_id: CF.MILEAGE_MAX, values: [{ value: used_mileage }] });
    if (CF.DELIVERY_CITY && city) cfs.push(cfValue(CF.DELIVERY_CITY, city));
    if (CF.CITY && city) cfs.push(cfValue(CF.CITY, city));

    const countryTitle = (COUNTRIES.find(x => x.code === country)?.title) || country || '';
    if (CF.COUNTRY_TITLE && countryTitle) cfs.push(cfValue(CF.COUNTRY_TITLE, countryTitle));

    if (CF.COMMENT_TEXT && (comment || comment === '')) cfs.push(cfValue(CF.COMMENT_TEXT, comment || ''));
    const methodMap = { phone: 'Телефон', wa: 'WhatsApp', tg: 'Telegram' };
    if (CF.CONTACT_METHOD && payload.contact_method) cfs.push(cfValue(CF.CONTACT_METHOD, methodMap[payload.contact_method] || payload.contact_method));
    if (CF.CONTACT_PHONE && contact?.phone_e164) cfs.push(cfValue(CF.CONTACT_PHONE, contact.phone_e164));
    if (CF.CONTACT_TG && contact?.tg_username) cfs.push(cfValue(CF.CONTACT_TG, '@' + contact.tg_username));

    const name = `Кальк • ${bm || 'Авто'} • ${countryTitle} → ${city || ''}`;

    const body = [{
      name,
      pipeline_id,
      status_id,
      ...(responsible_id ? { responsible_user_id: responsible_id } : {}),
      ...(cfs.length ? { custom_fields_values: cfs } : {})
    }];

    const resp = await api('POST', `/api/v4/leads`, body);
    return resp._embedded.leads[0].id;
  }

  async function linkContactToLead(contact_id, lead_id) {
    const body = [{ to_entity_id: contact_id, to_entity_type: 'contacts' }];
    await api('POST', `/api/v4/leads/${lead_id}/link`, body);
  }
  async function addTask(lead_id, text, completeAtTs) {
    const body = [{
      text,
      complete_till: Math.floor(completeAtTs / 1000),
      entity_id: lead_id,
      entity_type: 'leads',
      task_type_id: 1
    }];
    await api('POST', `/api/v4/tasks`, body);
  }
  async function addNote(lead_id, text) {
    const body = [{ note_type: 'common', params: { text } }];
    await api('POST', `/api/v4/leads/${lead_id}/notes`, body);
  }

  return {
    createOrUpdateContact,
    addContactNote,
    createLeadCalc,
    linkContactToLead,
    addTask,
    addNote,
    accessToken: null,
    expiresAt: 0
  };
})();

// ----------------- Telegram бот -----------------
if (!BOT_TOKEN) { LOG.error('BOT_TOKEN is missing'); process.exit(1); }
const bot = new Telegraf(BOT_TOKEN);

// ----------------- Хелперы UI ----------------
async function ensureMasterMessage(ctx, s) {
  if (s.master && s.master.chat_id && s.master.message_id) return s.master;
  const m = await ctx.reply('Загрузка…');
  s.master = { chat_id: m.chat.id, message_id: m.message_id };
  return s.master;
}

bot.use((ctx, next) => {
  ctx.replyHTML = (text, extra = {}) =>
    ctx.reply(text, { parse_mode: 'HTML', disable_web_page_preview: true, ...extra });
  return next();
});

const sendHTML = (chatId, text, extra = {}) =>
  bot.telegram.sendMessage(chatId, text, { parse_mode: 'HTML', disable_web_page_preview: true, ...extra });

async function deleteMasterIfAny(ctx, s) {
  if (s.master?.chat_id && s.master?.message_id) {
    try { await ctx.telegram.deleteMessage(s.master.chat_id, s.master.message_id); } catch {}
    s.master = null;
  }
}
async function safeEdit(ctx, m, text, markup, parse_mode) {
  try {
    await ctx.telegram.editMessageText(m.chat_id, m.message_id, undefined, text, {
      ...markup,
      ...(parse_mode ? { parse_mode } : {}),
      disable_web_page_preview: true
    });
  } catch (e) {
    if (!String(e).includes('message is not modified')) {
      LOG.warn({ e: String(e) }, 'editMessageText warn');
    }
  }
}
async function rebaseMaster(ctx, s, text, markup, parse_mode) {
  await deleteMasterIfAny(ctx, s);
  const m = await ctx.reply(text, {
    ...markup,
    ...(parse_mode ? { parse_mode } : {}),
    disable_web_page_preview: true
  });
  s.master = { chat_id: m.chat.id, message_id: m.message_id };
  return s.master;
}
function homeText() { return MSG_CALC_INTRO; }
function kbHome() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('🚀 Начать', 'cta:start')],
    [Markup.button.url('👨‍💼 Связаться с менеджером', MANAGER_TG_URL)],
  ]);
}
async function rebaseHome(ctx, s) {
  await rebaseMaster(ctx, s, homeText(), kbHome(), 'MarkdownV2');
}
async function notifyAndRebaseHome(ctx, s, text, extra = {}) {
  await ctx.reply(text, extra);
  await rebaseHome(ctx, s);
}

// ----------------- Рендеры калькулятора -----------------
function chipsCalc(s) {
  const arr = [];
  if (s.bm) arr.push(mdv2.esc(`Модель: ${s.bm}`));
  if (s.condition === 'new') arr.push(mdv2.esc('Состояние: Новый'));
  if (s.condition === 'used') {
    arr.push(mdv2.esc('Состояние: С пробегом'));
    if (s.used_year) arr.push(mdv2.esc(`Год: ${s.used_year}`));
    if (s.used_mileage) arr.push(mdv2.esc(`Пробег: до ${fmt(s.used_mileage)} км`));
  }
  if (s.city) arr.push(mdv2.esc(`Доставка: ${s.city}`));
  if (s.country) {
    const ct = COUNTRIES.find(x => x.code === s.country);
    if (ct) arr.push(mdv2.esc(`Страна: ${ct.flag} ${ct.title}`));
  }
  if (s.comment === '') arr.push(mdv2.esc('Комментарий: нет'));
  else if (s.comment) arr.push(mdv2.esc('Комментарий: есть'));
  if (s.await_text === 'bm') arr.push(mdv2.esc('Введите марку/модель латиницей…'));
  if (s.await_text === 'year') arr.push(mdv2.esc('Введите год выпуска…'));
  if (s.await_text === 'mileage') arr.push(mdv2.esc('Введите ограничение пробега…'));
  if (s.await_text === 'city_custom') arr.push(mdv2.esc('Введите город доставки…'));
  if (s.await_text === 'comment') arr.push(mdv2.esc('Введите комментарий…'));
  return arr.length ? `\n\n${arr.map(x => `• ${x}`).join('\n')}` : '';
}

function renderCalcPage(s) {
  const step = s.step || 1;
  let text = mdv2.esc(`🔧 Калькулятор • Шаг ${step}/7`) + `${chipsCalc(s)}`;
  let rows = [];

  if (step === 1) {
    text = [
      mdv2.esc('🔧 Калькулятор • Шаг 1/7'),
      '',
      MSG_QA_INTRO,
      '',
      MSG_ASK_MAKE_MODEL
    ].join('\n');
    s.await_text = 'bm';
    rows = [[Markup.button.callback('В меню', 'home')]];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  if (step === 2) {
    text = mdv2.esc(`🔧 Калькулятор • Шаг 2/7`) + `${chipsCalc(s)}\n\n${MSG_ASK_CONDITION}`;
    rows = [
      [Markup.button.callback('Новый', 'calc:cond:new'), Markup.button.callback('С пробегом', 'calc:cond:used')],
      [Markup.button.callback('↩ Назад', 'calc:back'), Markup.button.callback('В меню', 'home')]
    ];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  if (step === 3) {
    text = mdv2.esc(`🔧 Калькулятор • Шаг 3/7`) + `${chipsCalc(s)}\n\n` + mdv2.esc('Город доставки:');
    rows = [
      [Markup.button.callback('Москва', 'calc:city:msk'), Markup.button.callback('Санкт-Петербург', 'calc:city:spb')],
      [Markup.button.callback('Указать свой', 'calc:city:oth')],
      [Markup.button.callback('↩ Назад', 'calc:back')]
    ];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  if (step === 4) {
    text = mdv2.esc(`🔧 Калькулятор • Шаг 4/7`) + `${chipsCalc(s)}\n\n` + mdv2.esc('Страна вывоза:');
    rows = [
      COUNTRIES.slice(0,3).map(c => Markup.button.callback(`${c.flag} ${c.title}`, `calc:country:${c.code}`)),
      COUNTRIES.slice(3).map(c => Markup.button.callback(`${c.flag} ${c.title}`, `calc:country:${c.code}`)),
      [Markup.button.callback('↩ Назад', 'calc:back')]
    ];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  if (step === 5) {
    text = mdv2.esc(`🔧 Калькулятор • Шаг 5/7`) + `${chipsCalc(s)}\n\n` + mdv2.esc('Добавить комментарий?');
    rows = [
      [Markup.button.callback('Добавить комментарий', 'calc:comment:add')],
      [Markup.button.callback('Нет пожеланий', 'calc:comment:none')],
      [Markup.button.callback('↩ Назад', 'calc:back')]
    ];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  if (step === 6) {
    text = mdv2.esc('📦 Ориентировочная стоимость и сроки') + `${chipsCalc(s)}\n\n` +
      renderCostBlock(s) + '\n\n' +
      MSG_CUSTOMS_NOTE(s.bm || 'авто', s.city || 'Москве');
    rows = [[Markup.button.callback('Далее → Выбор канала связи', 'calc:to_contact')]];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  if (step === 7) {
    text =  mdv2.esc('Куда вам удобнее получить предложение?');
    rows = [
      [Markup.button.callback('Телефон', 'calc:cm:phone'), Markup.button.callback('Telegram', 'calc:cm:tg'), Markup.button.callback('WhatsApp', 'calc:cm:wa')],
      [Markup.button.callback('↩ Назад', 'calc:back'), Markup.button.callback('В меню', 'home')]
    ];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  return { text, markup: Markup.inlineKeyboard([[Markup.button.callback('В меню', 'home')]]) };
}

function renderCostBlock(s) {
  const c = COST[s.country];
  const bm = (s.bm && String(s.bm).trim()) || 'авто';
  const city = (s.city && String(s.city).trim()) || 'вашем городе';
  const countryTitle = COUNTRIES.find(x => x.code === s.country)?.title || '';
  const parts = [];
  if (countryTitle) parts.push(mdv2.esc(`Стоимость доставки авто из ${countryTitle} — ${fmt(c?.delivery || 0)} ₽.`));
  if (c?.days) parts.push(mdv2.esc(`Срок доставки: ~${c.days} дней.`));
  parts.push(mdv2.esc(`Услуги компании: ${fmt((c && c.service) || 150000)} ₽.`));
  parts.push(mdv2.esc(`Предоплата: 10%.`));
  parts.push('');
  parts.push(mdv2.esc(`Стоимость растаможки зависит от конкретного автомобиля.`));
  parts.push(mdv2.esc(`Давайте подберём для вас ${bm} и отправим вам готовое предложение «под ключ» в городе ${city}.`));
  return parts.join('\n');
}

// ----------------- Старт -----------------
bot.start(async (ctx) => {
  try {
    const arg = (ctx.message.text.split(' ').slice(1)[0] || '').trim();
    const s = S.get(ctx.from.id) || {};
    s.id = ctx.from.id;
    s.start_param = arg || 'organic';
    s.utm = parseUTM(arg);
    resetCalcState(s);
    S.set(ctx.from.id, s);
    await rebaseHome(ctx, s);
  } catch (e) {
    await notifyAndRebaseHome(ctx, {}, 'Упс, что-то пошло не так. Попробуйте ещё раз.');
  }
});

// ----------------- CTA -----------------
bot.action('home', async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  resetCalcState(s);
  await rebaseHome(ctx, s);
});

bot.action('cta:start', async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  resetCalcState(s);
  s.flow = 'calc';
  s.step = 1;
  s.flow_uid = ulid();        // важный идентификатор сессии
  s.submitted = false;        // сбрасываем флажок завершения
  S.set(ctx.from.id, s);
  const view = renderCalcPage(s);
  await rebaseMaster(ctx, s, view.text, view.markup, view.parse_mode);
});

bot.action('cta:manager', async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  resetCalcState(s);
  s.flow = 'manager';
  s.step = 7; // сразу на выбор канала связи
  s.flow_uid = ulid();
  s.submitted = false;
  const view = renderCalcPage(s);
  await rebaseMaster(ctx, s, view.text, view.markup);
});

// ----------------- FAQ -----------------
bot.action('faq', async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const { text, markup } = renderFaqSections();
  await safeEdit(ctx, m, text, markup);
});
bot.action(/^faq:sec:([^:]+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const secId = ctx.match[1];
  const { text, markup } = renderFaqSubs(secId);
  await safeEdit(ctx, m, text, markup);
});
bot.action(/^faq:sub:([^:]+):([^:]+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const secId = ctx.match[1], subId = ctx.match[2];
  const { text, markup } = renderFaqQuestions(secId, subId, 0);
  await safeEdit(ctx, m, text, markup);
});
bot.action(/^faq:list:([^:]+):([^:]+):?(\d+)?$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const secId = ctx.match[1], subId = ctx.match[2], page = Number(ctx.match[3] || 0);
  const { text, markup } = renderFaqQuestions(secId, subId, page);
  await safeEdit(ctx, m, text, markup);
});
bot.action(/^faq:q:(.+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id) || {};
  const m = await ensureMasterMessage(ctx, s);
  const qid = ctx.match[1];
  const { text, markup, parse_mode } = renderFaqAnswer(qid);
  await safeEdit(ctx, m, text, markup, parse_mode);
});

// ----------------- Калькулятор: инлайн-этапы -----------------
bot.action(/^calc:(.+)$/, async (ctx) => {
  await ctx.answerCbQuery();
  const s = S.get(ctx.from.id); if (!s) return;

  // если уже завершили и в «кулдауне» — игнорируем тап
  if (s.submitted && s.cooldown_until && Date.now() < s.cooldown_until) {
    return;
  }

  const [type, a] = ctx.match[1].split(':');

  if (type === 'cond') {
    s.condition = a; // new | used
    if (a === 'used') {
      s.await_text = 'year';
      await ctx.reply(mdv2.esc('Введите год выпуска (например: 2019).'), { parse_mode: 'MarkdownV2' });
      return;
    } else {
      s.used_year = null;
      s.used_mileage = null;
      s.step = 3;
    }
  }

  if (type === 'city') {
    if (a === 'msk') { s.city = 'Москва'; s.step = 4; }
    else if (a === 'spb') { s.city = 'Санкт-Петербург'; s.step = 4; }
    else if (a === 'oth') {
      s.await_text = 'city_custom';
      await ctx.reply(mdv2.esc('Введите город доставки (только буквы, пробелы и дефисы).'), { parse_mode: 'MarkdownV2' });
      return;
    }
  }

  if (type === 'country') {
    s.country = a; // EU/KR/CN/US/AE
    s.step = 5;
  }

  if (type === 'comment') {
    if (a === 'add') {
      s.await_text = 'comment';
      await ctx.reply(mdv2.esc('Введите комментарий одним сообщением.'), { parse_mode: 'MarkdownV2' });
      return;
    }
    if (a === 'none') {
      s.comment = '';
      s.await_text = null;
      await ctx.reply(
        mdv2.esc('📦 Ориентировочная стоимость и сроки') + '\n\n' + renderCostBlock(s),
        { parse_mode: 'MarkdownV2' }
      );
      s.step = 7;
      const view = renderCalcPage(s);
      await rebaseMaster(ctx, s, view.text, view.markup, view.parse_mode);
      return;
    }
  }

  if (type === 'to_contact') s.step = 7;

  if (type === 'cm') {
    s.contact_method = a; // phone | tg | wa
    if (a === 'phone' || a === 'wa') {
      await askContact(ctx, s);
      return;
    }
    if (a === 'tg') {
      if (ctx.from.username) {
        s.tg_username = ctx.from.username;
        await finalizeAndSend(ctx, s, { sendNew: true });
        return;
      } else {
        s.await_text = 'tg_username';
        await ctx.reply(mdv2.esc('Укажите ваш Telegram @username (например: @ivan_ivanov).'), { parse_mode: 'MarkdownV2' });
        return;
      }
    }
  }

  if (type === 'back') {
    if (s.step === 7) s.step = 5;
    else if (s.step > 1) s.step = s.step - 1;
  }
  if (type === 'next') {
    if (s.step < 7) s.step = s.step + 1;
  }

  const view = renderCalcPage(s);
  const m = await ensureMasterMessage(ctx, s);
  await safeEdit(ctx, m, view.text, view.markup, view.parse_mode);
});

// ----------------- Тексты от пользователя -----------------
bot.on('text', async (ctx) => {
  const s = S.get(ctx.from.id);
  if (!s || !s.await_text) return;
  const t = ctx.message.text.trim();

  if (s.await_text === 'bm') {
    if (!validateBrandModel(t)) {
      await ctx.reply(mdv2.esc('Неверный формат. Введите марку и модель латиницей (пример: BMW X5).'),
        { parse_mode: 'MarkdownV2' });
      return;
    }
    s.bm = t.toUpperCase();
    s.await_text = null;
    s.step = 2;
  } else if (s.await_text === 'year') {
    const y = parseYear(t);
    if (!y) {
      await ctx.reply(mdv2.esc('Неверный год. Введите 4 цифры, например: 2018.'), { parse_mode: 'MarkdownV2' });
      return;
    }
    s.used_year = y;
    s.await_text = 'mileage';
    await ctx.reply(mdv2.esc('Введите ограничение пробега (например: «до 50 000 км»).'), { parse_mode: 'MarkdownV2' });
    return;
  } else if (s.await_text === 'mileage') {
    const m = parseMileage(t);
    if (!m) {
      await ctx.reply(mdv2.esc('Неверный пробег. Пример: «до 50 000 км».'),
        { parse_mode: 'MarkdownV2' });
      return;
    }
    s.used_mileage = m;
    s.await_text = null;
    s.step = 3;
  } else if (s.await_text === 'city_custom') {
    const city = normalizeCityOrNull(t);
    if (!city) {
      await ctx.reply(mdv2.esc('Неверный формат города. Используйте только буквы, пробелы и дефисы.'),
        { parse_mode: 'MarkdownV2' });
      return;
    }
    s.city = city;
    s.await_text = null;
    s.step = 4;
  } else if (s.await_text === 'comment') {
    s.comment = t.slice(0, 800);
    s.await_text = null;

    await ctx.reply(
      mdv2.esc('📦 Ориентировочная стоимость и сроки') + '\n\n' + renderCostBlock(s),
      { parse_mode: 'MarkdownV2' }
    );

    s.step = 7;
    const v = renderCalcPage(s);
    await rebaseMaster(ctx, s, v.text, v.markup, v.parse_mode);
    return;
  } else if (s.await_text === 'tg_username') {
    const u = t.replace(/^@/, '');
    if (!/^[A-Za-z0-9_]{5,}$/.test(u)) {
      await ctx.reply(mdv2.esc('Неверный @username. Пример: @ivan_ivanov'), { parse_mode: 'MarkdownV2' });
      return;
    }
    s.tg_username = u;
    s.await_text = null;
    await finalizeAndSend(ctx, s, { sendNew: true });
    return;
  }

  const view = renderCalcPage(s);
  await rebaseMaster(ctx, s, view.text, view.markup, view.parse_mode);
});

// ----------------- Приём контакта (телефон) -----------------
bot.on('contact', async (ctx) => {
  const s = S.get(ctx.from.id);
  if (!s) return;
  const raw = String(ctx.message.contact.phone_number || '').replace(/[^\d+]/g, '');
  const phoneE164 = raw.startsWith('+') ? raw : (raw.startsWith('8') && raw.length === 11 ? '+7' + raw.slice(1) : '+' + raw);
  s.phone = phoneE164;
  s.contact_name = `${ctx.message.contact.first_name || ''} ${ctx.message.contact.last_name || ''}`.trim() || ctx.from.first_name || '';
  await ctx.reply('Спасибо! Контакт получен ✅', Markup.removeKeyboard());

  await finalizeAndSend(ctx, s, { sendNew: true });
});

// ----------------- Финализация -----------------
async function finalizeAndSend(ctx, s, { sendNew = false } = {}) {
  try {
    // анти-дабл-клик/гонка колбэков + кулдаун
    if (s.__finalizing) return;
    if (s.submitted && s.cooldown_until && Date.now() < s.cooldown_until) return;
    s.__finalizing = true;

    if (s.contact_method === 'tg' && !s.tg_username && !ctx.from.username) {
      s.await_text = 'tg_username';
      await ctx.reply(mdv2.esc('Нужен Telegram @username для связи. Укажите его сообщением (например: @ivan_ivanov).'),
        { parse_mode: 'MarkdownV2' });
      return;
    }
    if ((s.contact_method === 'phone' || s.contact_method === 'wa') && !s.phone) {
      await askContact(ctx, s);
      return;
    }

    const payload = buildPayloadFromState(ctx, s);
    const responsible_id = nextResponsible();
    queueAmoDelivery({ payload, responsible_id });

    const html  = toHtmlForTelegram(MSG_THANKS_SERVICES);
    const kb = Markup.inlineKeyboard([
      [Markup.button.url('Подписаться на AUTONEWS', NEWS_CHANNEL_URL)],
      [Markup.button.url('Каталог авто', CATALOG_URL)],
      [Markup.button.url('Проверка по VIN', VIN_BOT_URL)],
      [Markup.button.callback('🔁 Новый расчёт', 'cta:start'), Markup.button.callback('🏠 В меню', 'home')]
    ]);

    if (sendNew) {
      await ctx.reply(html, { ...kb, parse_mode: 'HTML', disable_web_page_preview: false });
    } else {
      const m = await ensureMasterMessage(ctx, s);
      await safeEdit(ctx, m, html, { ...kb }, 'HTML');
    }

    // помечаем завершение и ставим кулдаун, а сброс состояния — с задержкой
    s.submitted = true;
    s.cooldown_until = Date.now() + 10_000;
    const oldLeadUid = s.lead_uid;
    setTimeout(() => {
      // если за 10 сек не было новой сессии — сбросим
      if (s.lead_uid === oldLeadUid) resetCalcState(s);
    }, 10_000);
  } catch (e) {
    LOG.error(e, 'finalize error');
    await notifyAndRebaseHome(
      ctx,
      s,
      '⚠️ Не удалось отправить подтверждение в Telegram, но заявка поставлена в очередь в amoCRM. Повторим попытку.'
    );
  } finally {
    s.__finalizing = false;
  }
}

function buildPayloadFromState(ctx, s) {
  return {
    flow_uid: s.flow_uid || ulid(),        // добавили в полезную нагрузку
    lead_uid: s.lead_uid || ulid(),
    created_at: nowISO(),
    type: s.flow || 'calc',
    source: { start_param: s.start_param || 'organic', utm: s.utm || {} },
    contact: {
      phone_e164: s.phone || null,
      tg_username: s.tg_username || ctx.from.username || '',
      name: s.contact_name || `${ctx.from.first_name || ''} ${ctx.from.last_name || ''}`.trim()
    },
    bm: s.bm || '',
    condition: s.condition || '',
    used_year: s.used_year || null,
    used_mileage: s.used_mileage || null,
    city: s.city || '',
    country: s.country || '',
    comment: s.comment || '',
    consent: { accepted: true, version: '2025-09-01', ts: nowISO() }
  };
}

function queueAmoDelivery({ payload, responsible_id }) {
  const key = dedupKeyFromPayload(payload);
  if (dedupQueued(key) || dedupSeen(key)) {
    LOG.warn({ key }, 'duplicate lead suppressed (already queued/sent)');
    return;
  }
  markQueued(key);
  outbox.push({
    id: ulid(),
    t: 'amo.calc',
    dedup_key: key,
    payload,
    responsible_id: responsible_id || null,
    attempts: 0,
    next_at: Date.now()
  });
  saveJSON(OUTBOX_PATH, outbox);
}
setInterval(processOutboxTick, 2000);
setInterval(dedupCleanup, 60*60*1000); // часовой сборщик мусора

async function processOutboxTick() {
  const now = Date.now();
  for (const job of [...outbox]) {
    if (job.next_at > now) continue;
    try {
      if (job.t === 'amo.calc') {
        const k = job.dedup_key || dedupKeyFromPayload(job.payload);
        if (dedupSeen(k)) {
          const idx0 = outbox.findIndex(x => x.id === job.id);
          if (idx0 >= 0) outbox.splice(idx0, 1);
          saveJSON(OUTBOX_PATH, outbox);
          continue;
        }
        const res = await deliverCalcToAmo(job.payload, job.responsible_id);
        LOG.info({ res }, 'amoCRM lead created');
        markSent(k);
      }
      const idx = outbox.findIndex(x => x.id === job.id);
      if (idx >= 0) outbox.splice(idx, 1);
      saveJSON(OUTBOX_PATH, outbox);
    } catch (e) {
      job.attempts += 1;
      const backoffMs = attemptBackoff(job.attempts);
      job.next_at = Date.now() + backoffMs;
      saveJSON(OUTBOX_PATH, outbox);
      LOG.warn({ err: String(e), attempts: job.attempts, backoffMs }, 'amoCRM delivery error, will retry');
      if (job.attempts === 3 && ADMIN_CHAT_ID) {
        try { await bot.telegram.sendMessage(ADMIN_CHAT_ID, `⚠️ amoCRM недоступна, повтор через ${Math.round(backoffMs/1000)}с`); } catch {}
      }
    }
  }
}
function attemptBackoff(n) {
  if (n <= 1) return 60 * 1000;
  if (n === 2) return 5 * 60 * 1000;
  if (n === 3) return 15 * 60 * 1000;
  if (n === 4) return 60 * 60 * 1000;
  return 6 * 60 * 60 * 1000;
}

async function deliverCalcToAmo(payload, responsible_id) {
  const contact_id = await amo.createOrUpdateContact({
    phone_e164: payload.contact.phone_e164 || null,
    name: payload.contact.name,
    tg_username: payload.contact.tg_username
  });

  const lead_id = await amo.createLeadCalc({ payload, responsible_id });

  await amo.linkContactToLead(contact_id, lead_id);
  await amo.addNote(lead_id, calcSummary(payload));

  const completeAt = Date.now() + 15 * 60 * 1000;
  await amo.addTask(lead_id, 'Перезвонить/написать клиенту по калькулятору', completeAt);

  return { contact_id, lead_id };
}

function calcSummary(p) {
  const countryTitle = COUNTRIES.find(x => x.code === p.country)?.title || p.country || '-';
  const c = COST[p.country] || {};
  const lines = [
    `Калькулятор: ${p.bm || '-'}`,
    `Состояние: ${p.condition === 'new' ? 'Новый' : (p.condition === 'used' ? 'С пробегом' : '-')}`,
    ...(p.condition === 'used' ? [
      `Год: ${p.used_year || '-'}`,
      `Пробег: до ${p.used_mileage ? fmt(p.used_mileage) : '-'} км`
    ] : []),
    `Город доставки: ${p.city || '-'}`,
    `Страна вывоза: ${countryTitle}`,
    `Комментарий: ${p.comment ? 'есть' : 'нет'}`,
    '',
    `Ориентировочно: доставка ${c.delivery ? fmt(c.delivery) + ' ₽' : '-'}, срок ~${c.days || '?'} дн., услуги ${c.service ? fmt(c.service) + ' ₽' : '-'}.`,
    'Растаможка зависит от авто.',
    '',
    `Канал связи: ${p.contact?.phone_e164 ? 'Телефон/WhatsApp' : (p.contact?.tg_username ? '@' + p.contact.tg_username : '-')}`,
    `Источник: ${p.source?.start_param || '—'}`
  ];
  return lines.join('\n');
}

// ----------------- FAQ рендеры -----------------
function renderFaqSections() {
  const rows = [];
  const secs = FAQ.sections || [];
  for (const sec of secs) rows.push([Markup.button.callback(sec.title, `faq:sec:${sec.id}`)]);
  rows.push([Markup.button.callback('В меню', 'home')]);
  const text = '❓ Разделы ответов на вопросы:';
  return { text, markup: Markup.inlineKeyboard(rows) };
}
function renderFaqSubs(secId) {
  const sec = FAQIndex.secById.get(secId);
  if (!sec) {
    return {
      text: 'Раздел не найден.',
      markup: Markup.inlineKeyboard([[Markup.button.callback('К разделам', 'faq')]])
    };
  }
  const rows = [];
  for (const sub of sec.subs || []) {
    rows.push([Markup.button.callback(sub.title, `faq:sub:${secId}:${sub.id}`)]);
  }
  rows.push([Markup.button.callback('↩ К разделам', 'faq')]);
  const text = `❓ ${sec.title}\nВыберите подраздел:`;
  return { text, markup: Markup.inlineKeyboard(rows) };
}

function renderFaqQuestions(secId, subId, page = 0) {
  const key = `${secId}:${subId}`;
  const sub = FAQIndex.subByKey.get(key);
  const sec = FAQIndex.secById.get(secId);
  if (!sub || !sec) {
    return {
      text: 'Подраздел не найден.',
      markup: Markup.inlineKeyboard([[Markup.button.callback('↩ К подразделам', `faq:sec:${secId}`)]])
    };
  }

  const qs = sub.qs || [];
  const total = qs.length;
  const pages = Math.max(1, Math.ceil(total / FAQ_PAGE_SIZE));
  const p = Math.min(Math.max(0, page), pages - 1);
  const from = p * FAQ_PAGE_SIZE;
  const to = Math.min(from + FAQ_PAGE_SIZE, total);

  const rows = [];
  qs.slice(from, to).forEach(q => rows.push([Markup.button.callback(q.q, `faq:q:${q.id}`)]));

  const nav = [];
  if (p > 0) nav.push(Markup.button.callback('← Назад', `faq:list:${secId}:${subId}:${p - 1}`));
  if (p < pages - 1) nav.push(Markup.button.callback('Вперёд →', `faq:list:${secId}:${subId}:${p + 1}`));
  if (nav.length) rows.push(nav);

  rows.push([
    Markup.button.callback('↩ К подразделам', `faq:sec:${secId}`),
    Markup.button.callback('К разделам', 'faq')
  ]);

  const text = `❓ ${sec.title} → ${sub.title}\nВыберите вопрос:`;
  return { text, markup: Markup.inlineKeyboard(rows) };
}

function renderFaqAnswer(qid) {
  const q = FAQIndex.qById.get(qid);
  if (!q) {
    return {
      text: 'Вопрос не найден.',
      markup: Markup.inlineKeyboard([[Markup.button.callback('К разделам', 'faq')]])
    };
  }
  const text =
    mdv2.esc(`❓ ${q.secTitle} → ${q.subTitle}`) + '\n' +
    `*${mdv2.esc(q.q)}*` + '\n\n' +
    mdv2.esc(q.a);

  const rows = [
    [Markup.button.callback('↩ К вопросам', `faq:list:${q.secId}:${q.subId}:0`)],
    [Markup.button.callback('К подразделу', `faq:sub:${q.secId}:${q.subId}`)],
    [Markup.button.callback('К разделам', 'faq')],
    [Markup.button.callback('В меню', 'home')]
  ];
  return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
}

// ----------------- Вспомогательное -----------------
function resetCalcState(s) {
  s.flow = null;
  s.step = 0;
  s.await_text = null;

  s.flow_uid = ulid();       // новая сессия
  s.lead_uid = ulid();

  s.bm = null;
  s.condition = null;
  s.used_year = null;
  s.used_mileage = null;
  s.city = null;
  s.country = null;
  s.comment = null;          // null — не трогали; '' — «нет пожеланий»

  s.contact_method = null;
  s.phone = null;
  s.tg_username = null;
  s.contact_name = null;

  s.submitted = false;
  s.cooldown_until = 0;
  s.__finalizing = false;
}

function nextResponsible() {
  if (!RESPONSIBLES.length) return null;
  const idx = rr.i % RESPONSIBLES.length;
  rr.i = (rr.i + 1) % RESPONSIBLES.length;
  saveJSON(RR_PATH, rr);
  return RESPONSIBLES[idx];
}

async function askContact(ctx, s) {
  await ctx.reply(
    'Нажимая кнопку ниже, вы соглашаетесь на обработку персональных данных и обратную связь. Политика: https://example.com/privacy',
    Markup.keyboard([Markup.button.contactRequest('Отправить мой телефон')]).oneTime().resize()
  );
}

// ----------------- Запуск -----------------
bot.launch()
  .then(() => {
    LOG.info('LeadBot 2.0.1 started');
  })
  .catch(e => {
    LOG.error(e, 'bot.launch error');
    process.exit(1);
  });

process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
