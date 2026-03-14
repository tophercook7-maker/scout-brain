console.log("analyzer.js loaded");

async function auditWebsiteLive(url) {
  const apiBase = (window.MB_API_BASE || "").replace(/\/$/, "");
  const response = await fetch(`${apiBase}/audit`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url })
  });

  if (!response.ok) {
    const msg = response.status === 404 && window.MB_USE_CLOUD
      ? "Analyze endpoint not found. Set VITE_API_BASE_URL to your hosted backend URL."
      : "Audit endpoint did not respond correctly";
    throw new Error(msg);
  }

  return response.json();
}

function buildEmailFromAudit({ name, type, city, audit }) {
  const problems = audit.problems || [];
  const pitch = audit.pitch || [];

  const emailSubject = `Quick idea for ${name}'s website`;

  const bulletProblems = problems.map(x => `- ${x}`).join("\n");
  const bulletPitch = pitch.map(x => `- ${x}`).join("\n");

  const emailBody = `Hi there,

My name is Topher and I run MixedMakerShop.

I came across ${name}${city ? ` in ${city}` : ""} and had a quick idea that might help your website feel cleaner and easier to use on phones.

A few things stood out to me:
${bulletProblems || "- Your site could likely be made clearer and easier to use"}

I build simple modern websites for small businesses, and I think a better version of your site could:
${bulletPitch || "- create a cleaner mobile-friendly layout"}

If you'd like, I'd be happy to send over a quick example of what I mean.

No pressure at all — just wanted to reach out.

Thanks,
Topher
topher@mixedmakershop.com
MixedMakerShop.com`;

  return { emailSubject, emailBody };
}
