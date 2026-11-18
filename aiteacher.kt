package com.aitech.app.ai

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.concurrent.TimeUnit

/**
 * AI Teacher Core Engine
 * Powered by OpenRouter API (Default Model: deepseek/deepseek-r1:free)
 * Implements strict educational pedagogy, role enforcement, and SSE streaming.
 */
class AITeacherEngine(
    private var apiKey: String,
    private var selectedModel: String = "deepseek/deepseek-r1:free"
) {
    private val client = OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .build()

    companion object {
        private const val TAG = "AITeacherEngine"
        private const val OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

        const val EDUCATIONAL_SYSTEM_PROMPT = """
You are AI Teacher, a professional personal teacher.

Your ONLY primary purpose is teaching, education, student learning, academic support, and skill development.
You must teach students rather than simply giving answers.

TEACHING RULES:
1. Understand the student's question.
2. Identify the subject and topic.
3. Explain the concept in simple language.
4. Start with basic concepts before advanced concepts.
5. Use real-world examples.
6. Use step-by-step explanations for numerical problems.
7. Explain difficult terminology.
8. Use tables when they improve understanding.
9. Provide diagrams or structured text explanations when useful.
10. Give examples after explaining concepts.
11. Ask short practice questions when appropriate.
12. Correct student mistakes politely.
13. Never invent textbook facts or formulas.
14. If information is uncertain, explain the uncertainty.
15. Adapt explanations to the student's education level.

ANSWER FORMAT:
For educational questions, use this structure when appropriate:
1. Topic Name
2. Introduction
3. Simple Explanation
4. Important Concepts
5. Step-by-Step Explanation
6. Practical Examples
7. Important Points
8. Summary
9. Practice Questions

MATHEMATICS:
- Show formulas.
- Explain each calculation.
- Show all intermediate steps.
- Provide the final answer.
- Verify calculations where possible.

PROGRAMMING & NETWORKING:
- Explain the logic.
- Provide working code or configuration snippets.
- Explain important lines.
- Include output examples.
- Identify common errors.

SCIENCE:
- Explain scientific concepts accurately.
- Use examples from everyday life.
- Include formulas and units when applicable.

CONVERSATION BEHAVIOR:
- Be patient.
- Never insult students.
- Encourage independent learning.
- Do not unnecessarily repeat explanations.

If a user asks unrelated questions, politely guide the conversation back toward learning.
Your goal is to make every student understand the subject, not simply memorize answers.
"""
    }

    data class ChatMessage(
        val role: String, // "system", "user", "assistant"
        val content: String
    )

    fun updateApiKey(newKey: String) {
        this.apiKey = newKey
    }

    fun updateModel(newModel: String) {
        if (newModel.isNotBlank()) {
            this.selectedModel = newModel
        }
    }

    /**
     * Streams chat completion responses via Server-Sent Events (SSE).
     * Emits chunks of text as they arrive from OpenRouter.
     */
    fun streamChatCompletion(
        conversationHistory: List<ChatMessage>,
        studentProfileSummary: String = ""
    ): Flow<String> = flow {
        val messagesArray = JSONArray()

        // 1. Inject Educational System Prompt + Student Context
        val contextualSystemPrompt = buildString {
            append(EDUCATIONAL_SYSTEM_PROMPT)
            if (studentProfileSummary.isNotBlank()) {
                append("\n\nSTUDENT PROFILE & CONTEXT:\n$studentProfileSummary")
            }
        }

        messagesArray.put(JSONObject().apply {
            put("role", "system")
            put("content", contextualSystemPrompt)
        })

        // 2. Add conversation history
        for (msg in conversationHistory) {
            messagesArray.put(JSONObject().apply {
                put("role", msg.role)
                put("content", msg.content)
            })
        }

        val requestBodyJson = JSONObject().apply {
            put("model", selectedModel)
            put("messages", messagesArray)
            put("stream", true)
            put("temperature", 0.3) // Lower temperature for accurate educational responses
        }

        val body = requestBodyJson.toString()
            .toRequestBody("application/json; charset=utf-8".toMediaType())

        val request = Request.Builder()
            .url(OPENROUTER_URL)
            .addHeader("Authorization", "Bearer $apiKey")
            .addHeader("HTTP-Referer", "https://github.com/aitech/android-app")
            .addHeader("X-Title", "AI Teacher Android")
            .post(body)
            .build()

        try {
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    val errorBody = response.body?.string() ?: "Unknown error"
                    throw RuntimeException("API Error (${response.code}):$errorBody")
                }

                val source = response.body?.source() ?: throw RuntimeException("Response body is null")
                val reader = BufferedReader(InputStreamReader(source.inputStream()))
                
                var line: String? = reader.readLine()
                while (line != null) {
                    if (line.startsWith("data: ")) {
                        val jsonData = line.substring(6
