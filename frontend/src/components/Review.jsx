import Rating from "./Rating";

function Review({ author, date, rating, comment }) {
    return (
        <div className="border-b border-secondaryclr/20 py-4">
            <div className="flex items-center justify-between mb-2">
                <div>
                    <h4 className="font-semibold text-secondaryclr">{author}</h4>
                    <p className="text-sm text-secondaryclr/70">{date}</p>
                </div>
                <Rating rating={rating} />
            </div>
            <p className="text-secondaryclr">{comment}</p>
        </div>
    );
}

export default Review;
