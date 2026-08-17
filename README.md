# API Runner

Postman Runner-ga o'xshash universal API testing vositasi. CSV/JSON fayldan ma'lumot olib, har bir qatorni API ga yuboradi va natijalarni real vaqtda ko'rsatadi.

![Python](https://img.shields.io/badge/Python-3.8+-blue) ![Flask](https://img.shields.io/badge/Flask-SSE-green) ![License](https://img.shields.io/badge/license-MIT-lightgrey)

## Imkoniyatlar

- **Batch yuborish** — CSV yoki JSON fayldan ma'lumot olib, har bir qatorni API ga donalab yuboradi
- **`{{o'zgaruvchi}}` almashtiruv** — URL, header va body ichida ustun nomlarini avtomatik almashtiradi
- **Real-time natijalar** — Server-Sent Events orqali har bir so'rov natijasi darhol ko'rinadi
- **CURL parse** — URL maydoniga CURL buyrug'ini joylashtiring, barcha parametrlar avtomatik to'ldiriladi
- **Fayl ixtiyoriy** — faylsiz ham bitta so'rov yuborish mumkin
- **Saqlangan so'rovlar** — so'rovlarni papkalar bilan tartiblab saqlash imkoni
- **Split-pane interfeys** — natijalar o'ng panelda ochiladi, ajratuvchini sudrab o'lchamini o'zgartirish mumkin
- **RUN / STOP** — istalgan vaqtda to'xtatish imkoni

## O'rnatish

```bash
pip install flask requests
python api_runner.py
```

Brauzerda oching: [http://127.0.0.1:5050](http://127.0.0.1:5050)

## Ishlatish

### Oddiy so'rov
1. Method va URL kiriting
2. Kerak bo'lsa Header, Auth yoki Body qo'shing
3. **▶ RUN** tugmasini bosing

### Batch yuborish (Runner rejimi)
1. CSV yoki JSON fayl yuklang
2. URL yoki body ichida `{{ustun_nomi}}` yozing
3. **▶ RUN** — har bir qator uchun alohida so'rov yuboriladi

**CSV misol:**
```csv
user_id,token
101,abc123
102,def456
```

**URL misol:**
```
https://api.example.com/users/{{user_id}}
```

### CURL paste
URL maydoniga to'liq CURL buyrug'ini joylashtiring — method, headerlar, body avtomatik aniqlanadi.

```bash
curl -X POST 'https://api.example.com/data' \
  -H 'Authorization: Bearer token' \
  -d '{"key": "value"}'
```

## Texnologiyalar

- **Backend:** Python / Flask
- **Real-time:** Server-Sent Events (SSE)
- **Frontend:** Vanilla JS, CSS (framework yo'q)
- **Ma'lumotlar:** `saved_requests.json` (lokal)
