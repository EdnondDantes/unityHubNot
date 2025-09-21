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
const fetch = require('node-fetch');
const { ulid } = require('ulid');
const dayjs = require('dayjs');
const fs = require('fs');
const path = require('path');
const pino = require('pino');

// ----------------- Логгер -----------------
const LOG = pino({ level: process.env.LOG_LEVEL || 'info' });

// ----------------- Пути и файлы -----------------
const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
const OUTBOX_PATH = path.join(DATA_DIR, 'outbox.json');
const DEDUP_PATH = path.join(DATA_DIR, 'dedup.json');
const RR_PATH = path.join(DATA_DIR, 'rr.json');
const FAQ_PATH = path.join(__dirname, 'faq.json');

// ----------------- Конфиг окружения -----------------
const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_CHAT_ID = Number(process.env.ADMIN_CHAT_ID || 0);
const VIN_BOT_URL = process.env.VIN_BOT_URL || 'https://t.me';
const NEWS_CHANNEL_URL = process.env.NEWS_CHANNEL_URL || 'https://t.me';
const CATALOG_URL = process.env.CATALOG_URL || 'https://example.com';

// amoCRM
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
  DIRECTION: Number(process.env.AMO_CF_LEAD_DIRECTION_ID || 0),
  COUNTRY: Number(process.env.AMO_CF_LEAD_COUNTRY_ID || 0),
  DELIVERY_CITY: Number(process.env.AMO_CF_LEAD_DELIVERY_CITY_ID || 0),
  CITY: Number(process.env.AMO_CF_LEAD_CITY_ID || 0),
  START_PARAM: Number(process.env.AMO_CF_LEAD_START_PARAM_ID || 0),
  UTM_SOURCE: Number(process.env.AMO_CF_LEAD_UTM_SOURCE_ID || 0),
  UTM_MEDIUM: Number(process.env.AMO_CF_LEAD_UTM_MEDIUM_ID || 0),
  UTM_CAMPAIGN: Number(process.env.AMO_CF_LEAD_UTM_CAMPAIGN_ID || 0),
  UTM_CONTENT: Number(process.env.AMO_CF_LEAD_UTM_CONTENT_ID || 0),
  PD_VERSION: Number(process.env.AMO_CF_LEAD_PD_VERSION_ID || 0),
  PD_TS: Number(process.env.AMO_CF_LEAD_PD_TS_ID || 0)
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
const dedup  = loadJSON(DEDUP_PATH, {});
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
  try { if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch (e) { LOG.warn({ e }, `loadJSON error: ${file}`); }
  return fallback;
}
function saveJSON(file, data) { fs.writeFileSync(file, JSON.stringify(data, null, 2)); }
function nowISO() { return new Date().toISOString(); }
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
  async function refreshToken() {
    const url = `${AMO_BASE_URL}/oauth2/access_token`;
    const body = {
      client_id: AMO_CLIENT_ID,
      client_secret: AMO_CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: AMO_REFRESH_TOKEN,
      redirect_uri: AMO_REDIRECT_URI
    };
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify(body)
    });
    if (!res.ok) throw new Error(`AMO refresh failed: ${res.status} ${await res.text()}`);
    const data = await res.json();
    amo.accessToken = data.access_token;
    amo.expiresAt = Date.now() + (data.expires_in - 60) * 1000;
  }
  async function ensureToken() {
    if (!amo.accessToken || Date.now() > amo.expiresAt) await refreshToken();
  }
  async function api(method, path, body) {
    await ensureToken();
    const url = `${AMO_BASE_URL}${path}`;
    const res = await fetch(url, {
      method,
      headers: {
        'Authorization': `Bearer ${amo.accessToken}`,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: body ? JSON.stringify(body) : undefined
    });
    if (!res.ok) throw new Error(`AMO ${method} ${path} -> ${res.status}: ${await res.text()}`);
    if (res.status === 204) return null;
    return res.json();
  }
  function cfValue(field_id, value) { return { field_id, values: [{ value }] }; }

  async function createOrUpdateContact({ phone_e164, name, tg_username }) {
    // контакт без телефона (если связь через TG)
    if (!phone_e164) {
      const body = [{
        name: name || (tg_username ? `@${tg_username}` : 'Telegram lead'),
        custom_fields_values: [
          ...(AMO_CF_CONTACT_TELEGRAM_ID && tg_username ? [cfValue(AMO_CF_CONTACT_TELEGRAM_ID, `@${tg_username}`)] : [])
        ]
      }];
      const resp = await api('POST', `/api/v4/contacts`, body);
      return resp._embedded.contacts[0].id;
    }
    // поиск/обновление по телефону
    const q = encodeURIComponent(phone_e164);
    const data = await api('GET', `/api/v4/contacts?limit=1&query=${q}`);
    const found = data?._embedded?.contacts?.[0];
    const contactCF = [];
    if (AMO_CF_CONTACT_TELEGRAM_ID && tg_username) {
      contactCF.push(cfValue(AMO_CF_CONTACT_TELEGRAM_ID, `@${tg_username}`));
    }
    if (found) {
      const body = [{
        id: found.id,
        name: name || found.name || phone_e164,
        ...(contactCF.length ? { custom_fields_values: contactCF } : {})
      }];
      await api('PATCH', `/api/v4/contacts`, body);
      return found.id;
    } else {
      const body = [{
        name: name || phone_e164,
        custom_fields_values: [
          { field_code: 'PHONE', values: [{ value: phone_e164 }] },
          ...contactCF
        ]
      }];
      const resp = await api('POST', `/api/v4/contacts`, body);
      return resp._embedded.contacts[0].id;
    }
  }

  async function createLeadCalc({ payload, responsible_id }) {
    const { bm, condition, used_year, used_mileage, city, country, comment, source, consent } = payload;
    const pipeline_id = AMO_PIPELINE_IMPORT_ID;
    const status_id = AMO_STATUS_IMPORT_NEW_ID;

    const cfs = [];
    if (CF.DIRECTION) cfs.push(cfValue(CF.DIRECTION, 'calc'));
    if (CF.COUNTRY && country) cfs.push(cfValue(CF.COUNTRY, country));
    if (CF.DELIVERY_CITY && city) cfs.push(cfValue(CF.DELIVERY_CITY, city));
    if (CF.CITY && city) cfs.push(cfValue(CF.CITY, city));
    if (CF.START_PARAM && source?.start_param) cfs.push(cfValue(CF.START_PARAM, source.start_param));
    if (source?.utm) {
      if (CF.UTM_SOURCE && source.utm.source) cfs.push(cfValue(CF.UTM_SOURCE, source.utm.source));
      if (CF.UTM_MEDIUM && source.utm.medium) cfs.push(cfValue(CF.UTM_MEDIUM, source.utm.medium));
      if (CF.UTM_CAMPAIGN && source.utm.campaign) cfs.push(cfValue(CF.UTM_CAMPAIGN, source.utm.campaign));
      if (CF.UTM_CONTENT && source.utm.content) cfs.push(cfValue(CF.UTM_CONTENT, source.utm.content));
    }
    if (consent && CF.PD_VERSION && CF.PD_TS) {
      cfs.push(cfValue(CF.PD_VERSION, consent.version || ''));
      cfs.push(cfValue(CF.PD_TS, consent.ts || ''));
    }

    const countryTitle = COUNTRIES.find(x => x.code === country)?.title || country || '';
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

  return { createOrUpdateContact, createLeadCalc, linkContactToLead, addTask, addNote, accessToken: null, expiresAt: 0 };
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
function homeText() {
  return MSG_CALC_INTRO; // Markdown-V2 интро
}
function kbHome() {
  return Markup.inlineKeyboard([
    [Markup.button.callback('🚀 Начать', 'cta:start')],
    [Markup.button.callback('👨‍💼 Связаться с менеджером', 'cta:manager')],
    // [Markup.button.callback('❓ Ответы на популярные вопросы', 'faq')]
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
  if (s.comment) arr.push(mdv2.esc('Комментарий: есть'));
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
      [Markup.button.callback('↩ Назад', 'calc:back'), Markup.button.callback('Вперёд →', 'calc:next')]
    ];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  if (step === 4) {
    text = mdv2.esc(`🔧 Калькулятор • Шаг 4/7`) + `${chipsCalc(s)}\n\n` + mdv2.esc('Страна вывоза:');
    rows = [
      COUNTRIES.slice(0,3).map(c => Markup.button.callback(`${c.flag} ${c.title}`, `calc:country:${c.code}`)),
      COUNTRIES.slice(3).map(c => Markup.button.callback(`${c.flag} ${c.title}`, `calc:country:${c.code}`)),
      [Markup.button.callback('↩ Назад', 'calc:back'), Markup.button.callback('Вперёд →', 'calc:next')]
    ];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  if (step === 5) {
    text = mdv2.esc(`🔧 Калькулятор • Шаг 5/7`) + `${chipsCalc(s)}\n\n` + mdv2.esc('Добавить комментарий?');
    rows = [
      [Markup.button.callback('Добавить комментарий', 'calc:comment:add')],
      [Markup.button.callback('Нет пожеланий', 'calc:comment:none')],
      [Markup.button.callback('↩ Назад', 'calc:back'), Markup.button.callback('Вперёд →', 'calc:next')]
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
    text = mdv2.esc('📨 Канал для отправки персонального предложения') + `${chipsCalc(s)}\n\n` +
      mdv2.esc('Выберите удобный канал:');
    rows = [
      [Markup.button.callback('Телефон', 'calc:cm:phone')],
      [Markup.button.callback('Telegram', 'calc:cm:tg')],
      [Markup.button.callback('WhatsApp', 'calc:cm:wa')],
      [Markup.button.callback('↩ Назад', 'calc:back'), Markup.button.callback('В меню', 'home')]
    ];
    return { text, markup: Markup.inlineKeyboard(rows), parse_mode: 'MarkdownV2' };
  }

  return { text, markup: Markup.inlineKeyboard([[Markup.button.callback('В меню', 'home')]]) };
}

function renderCostBlock(s) {
  const c = COST[s.country];
  const countryTitle = COUNTRIES.find(x => x.code === s.country)?.title || '';
  const parts = [];
  if (countryTitle) parts.push(mdv2.esc(`Стоимость доставки авто из ${countryTitle} — ${fmt(c?.delivery || 0)} ₽.`));
  if (c?.days) parts.push(mdv2.esc(`Срок доставки: ~${c.days} дней.`));
  parts.push(mdv2.esc(`Услуги компании: ${fmt((c && c.service) || 150000)} ₽.`));
  parts.push(mdv2.esc(`Предоплата: 10%.`));
  parts.push('');
  parts.push(mdv2.esc(`Стоимость растаможки зависит от конкретного автомобиля.`));
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
  const [type, a] = ctx.match[1].split(':');

  if (type === 'cond') {
    s.condition = a; // new | used
    if (a === 'used') {
      s.await_text = 'year';
      await ctx.reply(mdv2.esc('Введите год выпуска (например: 2019).'), { parse_mode: 'MarkdownV2' });
      return; // не перерисовываем меню
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
      return; // ждём ввод
    }
  }

  if (type === 'country') {
    s.country = a; // EU/KR/CN/US/AE
  }

  if (type === 'comment') {
    if (a === 'add') {
      s.await_text = 'comment';
      await ctx.reply(mdv2.esc('Введите комментарий одним сообщением.'), { parse_mode: 'MarkdownV2' });
      return; // ждём ввод
    }
    if (a === 'none') {
      s.comment = '';
      s.step = 6;
    }
  }

  if (type === 'to_contact') {
    s.step = 7;
  }

  if (type === 'cm') {
    s.contact_method = a; // phone | tg | wa
    if (a === 'phone' || a === 'wa') {
      await askContact(ctx, s);
      return; // ждём контакт
    }
    if (a === 'tg') {
      if (ctx.from.username) {
        s.tg_username = ctx.from.username;
        await finalizeAndSend(ctx, s, { sendNew: true });
        return;
      } else {
        s.await_text = 'tg_username';
        await ctx.reply(mdv2.esc('Укажите ваш Telegram @username (например: @ivan_ivanov).'), { parse_mode: 'MarkdownV2' });
        return; // ждём ввод
      }
    }
  }

  if (type === 'back') {
    if (s.step > 1) s.step = s.step - 1;
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
      return; // меню не показываем
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
    return; // ждём пробег, меню не показываем
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
    s.step = 6;

    // «Просто сообщение» с расчётом (Markdown-V2)
    await ctx.reply(
      mdv2.esc('📦 Ориентировочная стоимость и сроки') + '\n\n' + renderCostBlock(s),
      { parse_mode: 'MarkdownV2' }
    );

    // сразу следующий этап — выбор канала
    s.step = 7;
    const v = renderCalcPage(s);
    await rebaseMaster(ctx, s, v.text, v.markup, v.parse_mode);
    return;
  } else if (s.await_text === 'tg_username') {
    const u = t.replace(/^@/, '');
    if (!/^[A-Za-z0-9_]{5,}$/.test(u)) {
      await ctx.reply(mdv2.esc('Неверный @username. Пример: @ivan_ivanов'), { parse_mode: 'MarkdownV2' });
      return;
    }
    s.tg_username = u;
    s.await_text = null;
    await finalizeAndSend(ctx, s, { sendNew: true });
    return;
  }

  // после валидного ввода — переносим меню вниз
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
    if (s.contact_method === 'tg' && !s.tg_username && !ctx.from.username) {
      s.await_text = 'tg_username';
      await ctx.reply(mdv2.esc('Нужен Telegram @username для связи. Укажите его сообщением (например: @ivan_ivanов).'),
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

    // Спасибо-экран (Markdown-V2 со ссылками)
    const lines = MSG_THANKS_SERVICES;

    const kb = Markup.inlineKeyboard([
      [Markup.button.url('Подписаться на AUTONEWS', NEWS_CHANNEL_URL)],
      [Markup.button.url('Каталог авто', CATALOG_URL)],
      [Markup.button.url('Проверка по VIN', VIN_BOT_URL)],
      [Markup.button.callback('🔁 Новый расчёт', 'cta:start'), Markup.button.callback('🏠 В меню', 'home')]
    ]);

    if (sendNew) {
      await ctx.reply(lines, { ...kb, parse_mode: 'MarkdownV2', disable_web_page_preview: false });
    } else {
      const m = await ensureMasterMessage(ctx, s);
      await safeEdit(ctx, m, lines, { ...kb }, 'MarkdownV2');
    }

    resetCalcState(s);
  } catch (e) {
    LOG.error(e, 'finalize error');
    await notifyAndRebaseHome(ctx, s, 'Не получилось отправить заявку сразу. Мы повторим попытку автоматически.');
  }
}

function buildPayloadFromState(ctx, s) {
  return {
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
  outbox.push({
    id: ulid(),
    t: 'amo.calc',
    payload,
    responsible_id: responsible_id || null,
    attempts: 0,
    next_at: Date.now()
  });
  saveJSON(OUTBOX_PATH, outbox);
}
setInterval(processOutboxTick, 2000);

async function processOutboxTick() {
  const now = Date.now();
  for (const job of [...outbox]) {
    if (job.next_at > now) continue;
    try {
      if (job.t === 'amo.calc') {
        await deliverCalcToAmo(job.payload, job.responsible_id);
      }
      const idx = outbox.findIndex(x => x.id === job.id);
      if (idx >= 0) outbox.splice(idx, 1);
      saveJSON(OUTBOX_PATH, outbox);
    } catch (e) {
      job.attempts += 1;
      const backoffMs = attemptBackoff(job.attempts);
      job.next_at = Date.now() + backoffMs;
      saveJSON(OUTBOX_PATH, outbox);
      if (job.attempts === 3 && ADMIN_CHAT_ID) {
        try { await bot.telegram.sendMessage(ADMIN_CHAT_ID, `⚠️ amoCRM недоступна, повторная попытка через ${Math.round(backoffMs/1000)}с`); } catch {}
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
  if (!sec) return { text: 'Раздел не найден.', markup: Markup.inlineKeyboard([[Markup.button.callback('К разделам', 'faq')]]) };
  const rows = [];
  for (const sub of sec.subs || []) rows.push([Markup.button.callback(sub.title, `faq:sub:${secId}:${sub.id}`)]);
  rows.push([Markup.button.callback('↩ К разделам', 'faq')]);
  const text = `❓ ${sec.title}\nВыберите подраздел:`;
  return { text, markup: Markup.inlineKeyboard(rows) };
}
function renderFaqQuestions(secId, subId, page = 0) {
  const key = `${secId}:${subId}`;
  const sub = FAQIndex.subByKey.get(key);
  const sec = FAQIndex.secById.get(secId);
  if (!sub || !sec) return { text: 'Подраздел не найден.', markup: Markup.inlineKeyboard([[Markup.button.callback('↩ К подразделам', `faq:sec:${secId}`)]]) };

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

  rows.push([Markup.button.callback('↩ К подразделам', `faq:sec:${secId}`), Markup.button.callback('К разделам', 'faq')]);

  const text = `❓ ${sec.title} → ${sub.title}\nВыберите вопрос:`;
  return { text, markup: Markup.inlineKeyboard(rows) };
}
function renderFaqAnswer(qid) {
  const q = FAQIndex.qById.get(qid);
  if (!q) return { text: 'Вопрос не найден.', markup: Markup.inlineKeyboard([[Markup.button.callback('К разделам', 'faq')]]) };
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
  s.lead_uid = ulid();
  s.bm = null;
  s.condition = null;
  s.used_year = null;
  s.used_mileage = null;
  s.city = null;
  s.country = null;
  s.comment = null;
  s.contact_method = null;
  s.phone = null;
  s.tg_username = null;
  s.contact_name = null;
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
bot.launch().then(() => {
  LOG.info('LeadBot 2.0.1 started');
}).catch(e => {
  LOG.error(e, 'bot.launch error');
  process.exit(1);
});
process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));
