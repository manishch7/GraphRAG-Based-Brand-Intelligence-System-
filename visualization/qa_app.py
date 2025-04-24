import streamlit as st
from llm_qa import QASystem  # Import from our simplified QA system

# Page setup
st.set_page_config(page_title="Brand Analytics Q&A", layout="wide")
st.title("Sportswear Brand Analytics Q&A")
st.write("Ask questions about Nike, Adidas, Puma and other sportswear brands")

# Initialize QA system
@st.cache_resource
def get_qa_system():
    return QASystem()

qa = get_qa_system()

# Initialize chat history in session state if it doesn't exist
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Main layout with two columns
col1, col2 = st.columns([3, 1])

with col2:
    # Sidebar with chat history
    st.header("Conversation History")
    
    # Display chat history
    for i, chat in enumerate(st.session_state.chat_history):
        if st.button(f"Q: {chat['question'][:30]}...", key=f"history_{i}"):
            st.session_state.selected_thread = i
    
    # Clear history button
    if st.button("Clear History"):
        st.session_state.chat_history = []
        if "selected_thread" in st.session_state:
            del st.session_state.selected_thread
    
    # Sample questions
    st.header("Sample Questions")
    sample_questions = [
        "Which brand has the most positive sentiment?",
        "What are people saying about Nike running shoes?",
        "Compare Adidas and Puma"
    ]
    
    for i, question in enumerate(sample_questions):
        if st.button(question, key=f"sample_{i}"):
            st.session_state.current_question = question

with col1:
    # User input area
    question = st.text_input(
        "Your question:", 
        value=st.session_state.get("current_question", ""),
        key="question_input"
    )
    
    # Process new question
    if st.button("Ask") and question:
        with st.spinner("Analyzing tweets..."):
            # Process the question
            result = qa.process_question(question)
            
            # Add to chat history
            st.session_state.chat_history.append({
                "question": question,
                "answer": result["answer"],
                "sources": result["sources"][:10]  # Store top 10 sources
            })
            
            # Auto-select the new thread
            st.session_state.selected_thread = len(st.session_state.chat_history) - 1
            
            # Clear the input
            st.session_state.current_question = ""
    
    # Display selected conversation
    if "selected_thread" in st.session_state and st.session_state.chat_history:
        thread_idx = st.session_state.selected_thread
        
        if 0 <= thread_idx < len(st.session_state.chat_history):
            thread = st.session_state.chat_history[thread_idx]
            
            # Display question and answer
            st.header("Question")
            st.write(thread["question"])
            
            st.header("Answer")
            st.write(thread["answer"])
            
            # Display sources
            with st.expander(f"View Sources ({len(thread['sources'])} tweets)"):
                for i, source in enumerate(thread["sources"], 1):
                    # Tweet container
                    st.markdown(f"""
                    **{i}. @{source.get('user', 'Anonymous')}**
                    
                    {source.get('tweet', '')}
                    
                    *Sentiment: {source.get('sentiment', 'Unknown')} | 
                    Likes: {source.get('like_count', 0)} | 
                    Retweets: {source.get('retweet_count', 0)}*
                    
                    ---
                    """)
            
            # Follow-up question
            st.header("Follow-up Question")
            follow_up = st.text_input("", key=f"follow_up_{thread_idx}")
            
            if st.button("Send Follow-up", key=f"send_follow_up_{thread_idx}") and follow_up:
                with st.spinner("Analyzing tweets..."):
                    # Process the follow-up
                    result = qa.process_question(follow_up)
                    
                    # Add to chat history
                    st.session_state.chat_history.append({
                        "question": follow_up,
                        "answer": result["answer"],
                        "sources": result["sources"][:10]
                    })
                    
                    # Auto-select the new thread
                    st.session_state.selected_thread = len(st.session_state.chat_history) - 1
        else:
            st.info("Select a conversation from the history")
    else:
        # Welcome message when no conversation is selected
        st.info("""
        Welcome! Use this tool to ask questions about sportswear brands based on Twitter data.
        
        Try asking about:
        - Brand sentiment and customer opinions
        - Product feedback and comparisons
        - Current trends and topics
        """)